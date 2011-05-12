# Copyright (c) 2009-2011 VMware, Inc.
require 'base/provisioner'
require 'maglev_service/common'

class VCAP::Services::Maglev::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Maglev::Common

  def node_score(node)
    10  # TODO
  end
end

