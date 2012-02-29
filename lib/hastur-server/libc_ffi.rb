require "ffi"

module LibC
  extend FFI::Library
  ffi_lib FFI::Library::LIBC

  RUSAGE_SELF = 0
  RUSAGE_CHILDREN = -1
  RUSAGE_THREAD = 1 # Linux (glibc?) only

  # Shamelessly borrowed from Jordan Sissel's Parasite:
  # https://raw.github.com/jordansissel/parasite/master/sinatra-example.rb
  class Timeval < FFI::Struct
    layout :tv_sec, :long,
           :tv_usec, :long
  end

  class RUsage < FFI::Struct
    layout :ru_utime, Timeval,
           :ru_stime, Timeval,
           :ru_maxrss, :long,
           :ru_ixrss, :long,
           :ru_idrss, :long,
           :ru_isrss, :long,
           :ru_minflt, :long,
           :ru_majflt, :long,
           :ru_nswap, :long,
           :ru_inblock, :long,
           :ru_oublock, :long,
           :ru_msgsnd, :long,
           :ru_msgrcv, :long,
           :ru_nsignals, :long,
           :ru_nvcsw, :long,
           :ru_nivcsw, :long
  end

  # sys/resource.h: int getrusage(int who, struct rusage *r_usage);
  attach_function :sys_getrusage, :getrusage, [:int, :pointer], :int

  # e.g.
  # usertime = rusage[:ru_utime][:tv_sec] + (rusage[:ru_utime][:tv_usec] / 1000000.0)
  # systime = rusage[:ru_stime][:tv_sec] + (rusage[:ru_stime][:tv_usec] / 1000000.0)
  def self.getrusage(who=RUSAGE_SELF)
    rusage = RUsage.new
    ret = sys_getrusage(who, rusage)
    raise Errno::EINVAL.exception unless ret == 0
    rusage
  end

  # unistd.h: unsigned alarm(unsigned seconds);
  attach_function :alarm, [ :uint ], :uint
end
