require "rubygems"
require "socket"
require "json"
require "uuid"

#
# Library to retrieve machine information that is in Hastur compliant JSON
#
module MachineInfo
  def self.get_machine_info
    info = Hash.new
    info["name"] = UUID.new.generate
    info["hostname"] = Socket.gethostname
    info["ipv4"] = IPSocket::getaddress(info["hostname"])
    info
  end
end
