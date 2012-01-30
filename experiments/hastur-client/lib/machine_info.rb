require "rubygems"
require "socket"
require "json"
require "uuid"

#
# Library to retrieve machine information that is in Hastur compliant JSON
#
module MachineInfo
  def self.get_machine_info( uuid )
    info = Hash.new
    info["name"] = uuid
    info["hostname"] = Socket.gethostname
    info["ipv4"] = IPSocket::getaddress(info["hostname"])
    info
  end
end
