# Copyright (c) 2009-2011 VMware, Inc.

# TODO:
#
# + We are not deleting provisioned maglev stone entries out of the DB.
#   when we start up, we try to start a ton of old, removed stones....
#
# + Doing a provision does not leave the service registered.
#
# 1. Allow configuration of the new stone via the config/*.yml files.
#    Might use erb for this.  Esp passwords/users
#
# 2. Need to settle on a metric and implement it (e.g., memory).
#
# 3. It seems like ProvisionedService#kill() is getting called a lot on
#    shutdown...Perhaps entries are not getting deleted out of the DB?:
#
#      ==== [2011-04-18 12:39:38 -0700] VCAP::Services::Maglev::Node ==== Shutting down instances..
#      ==== [2011-04-18 12:39:38 -0700] ..Maglev::Node::ProvisionedService ==== pid: 3649 running?: false
#      ==== [2011-04-18 12:39:38 -0700] ..Maglev::Node::ProvisionedService ==== pid: 19409 running?: false
#      ...
require 'fileutils'
require 'logger'
require 'pp'
require 'set'

require 'datamapper'
require 'nats/client'
require 'uuidtools'

require 'vcap/common'
require 'vcap/component'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Maglev
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require 'maglev_service/common'
require 'maglev_service/maglev_provisioned_service'

class VCAP::Services::Maglev::Node

  include VCAP::Services::Maglev::Common

  def initialize(options)
    super(options)  # handles @node_id, @logger, @local_ip, @node_nats

    @available_memory = options[:available_memory]
    @base_dir         = options[:base_dir]
    @max_memory       = options[:max_memory]
    @local_db         = options[:local_db]
    @maglev_home      = options[:maglev_home]

    raise "Maglev home not set: #{options.inspect}" unless @maglev_home

    FileUtils.mkdir_p(@base_dir) if @base_dir
    start_db

    # A Hack to ensure provisioned stones are running.  Since there is
    # no officially supported life-cycle hook to start a provisioned
    # service process during app start up, we just ensure that all
    # provisoined stones on a node start when the node starts up.
    ProvisionedService.all.each { |provisioned_service| provisioned_service.start_stone }
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |provisioned_service| provisioned_service.kill(:SIGTERM) }
  end

  def announcement
    a = {
      :available_memory => @available_memory,
    }
  end

  def provision(plan)
    start_time = Time.now
    service             = ProvisionedService.new
    service.memory      = @max_memory
    service.name        = "maglev-#{UUIDTools::UUID.random_create.to_s}"
    service.plan        = plan
    service.maglev_home = @maglev_home

    # This is last, after all other attributes are set
    service.pid         = start_instance(service)

    reduced_memory = true
    @available_memory -= service.memory

    unless service.save
      cleanup_service(service)
      raise "Could not save entry: #{service.errors.pretty_inspect}"
    end

    # All of the fields in this response will be stored in the DB
    # under the entry for the particular provisioned service.  Later,
    # when bind() is called, we can retrieve that information and use
    # it to bind.  So make sure any info we need to know about the
    # provisioned service is included in this info.
    response = {
      "hostname" => @local_ip,
      "name"     => service.name,
      "pid"      => service.pid,
    }
    provisioning_time = Time.now - start_time

    @logger.debug("provision(): response: #{response} (time: #{provisioning_time}")
    return response
  rescue => e
    @available_memory += service.memory if reduced_memory
    @logger.warn(e)
    raise e
  end

  def unprovision(name, bindings)
    service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if service.nil?

    cleanup_service(service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end

  def cleanup_service(service)
    @logger.debug("Killing #{service.name} started with pid #{service.pid}")
    # destroy() removes the entry from the DB
    raise "Could not cleanup service: #{service.errors.pretty_inspect}" unless service.destroy

    service.kill
    service.remove_stone_files

    dir = File.join(@base_dir, service.name)

    EM.defer { FileUtils.rm_rf(dir) }
    true
  rescue => e
    @logger.warn(e)
    raise e
  end

  # Bind a service.  The name is the name of the service.  But we
  # don't know what the app is???
  def bind(name, bind_opts)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")

    service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if service.nil?
    # The service will be an instance of
    # VCAP::Services::Maglev::Node::ProvisionedService,
    # and look like:
    #
    # #<VCAP::Services::Maglev::Node::ProvisionedService
    #   @memory=512
    #   @name="maglev-b28044e3-2f6e-4e0d-8093-44a2f32b4857"
    #   @pid=19395
    #   @plan=:free
    #   @maglev_home="/home/maglev/Maglev/maglev">
    #
    # The pid is the stoned pid.  We can call methods on the
    # provisioned service to check if it is running etc.

    #    username = UUIDTools::UUID.random_create.to_s
    #    password = UUIDTools::UUID.random_create.to_s

    response = {
      "hostname" => @local_ip,
      "stonename" => name,
      #      "port"    => service.port,
      # "username" => username,
      # "password" => password,
      # "name"     => service.name,
      # "db"       => service.db
    }

    @logger.debug("response: #{response}")
    response
  rescue => e
    @logger.warn(e)
    nil
  end

  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if service.nil?

    @logger.debug("Successfully unbind #{credentials}")
  rescue => e
    @logger.warn(e)
    nil
  end

  def start
    @logger.debug("start(): NOT IMPLEMENTED")
  end

  # Start an instance of the stone running on this machine.
  # Return the stoned's pid.  This will be used to monitor health of stone.
  # (and may be used to kill it?).
  def start_instance(service)
    @logger.debug("start_instance(): Starting: #{service.pretty_inspect}")

    # Do the creation of the files before the fork, so that the directories
    # and files are guaranteed to exist before we call waitstone in the
    # parent.
    service.create_stone_files

    pid = fork
    if pid
      # In parent, detach the child.
      Process.detach(pid)
      stone_pid = service.waitstone
      @logger.debug("Service #{service.name} started with pid #{stone_pid}")
      stone_pid
    else
      $0 = "Starting Maglev service: #{service.name}"
      # close_fds  # Seems to give a log of EINVALs

      dir = File.join(@base_dir, service.name)
      data_dir = File.join(dir, "data")
      log_file = File.join(dir, "log")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)

      service.exec_start_stone

      raise "Exec failed....#{$?.inspect}"
    end
  end

  def memory_for_service(service)
    case service.plan
    when :free then 256
    else
      raise "Invalid plan: #{service.plan}"
    end
  end
end
