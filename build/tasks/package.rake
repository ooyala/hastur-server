require 'rake/clean'
require 'httparty'
require 'hastur-server/version'

# HTTParty blindly inflates tgz files if the webserver sets the encoding.
module HTTParty
  class Request
    def handle_deflation
    end
  end
end

namespace :hastur do
  PATHS = {
    :prefix => "/opt/hastur",
    :build  => "/opt/hastur/build",
    :bindir => "/opt/hastur/bin",
    :libdir => "/opt/hastur/lib",
    :incdir => "/opt/hastur/include",
  }

  PACKAGES = [ :zlib, :openssl, :yaml, :libffi, :ruby, :zeromq ]
  VERSIONS = {
    :zlib    => "http://zlib.net/zlib-1.2.6.tar.gz",
    :openssl => "http://www.openssl.org/source/openssl-1.0.1a.tar.gz",
    :yaml    => "http://pyyaml.org/download/libyaml/yaml-0.1.4.tar.gz",
    :libffi  => "http://al-dev1.sv2/libffi-3.0.11.tar.gz", # temporary, upstream does not have an http url
    :ruby    => "http://ftp.ruby-lang.org/pub/ruby/1.9/ruby-1.9.3-p194.tar.gz",
    :zeromq  => "http://download.zeromq.org/zeromq-2.2.0.tar.gz",
  }
  CHECKSUMS = {
    :zlib    => "618e944d7c7cd6521551e30b32322f4a",
    :openssl => "a0104320c0997cd33e18b8ea798609d1",
    :yaml    => "36c852831d02cf90508c29852361d01b",
    :libffi  => "f69b9693227d976835b4857b1ba7d0e3",
    :ruby    => "bc0c715c69da4d1d8bd57069c19f6c0e",
    :zeromq  => "1b11aae09b19d18276d0717b2ea288f6",
  }

  GEM_INSTALL = "#{File.join(PATHS[:bindir], 'gem')} install --bindir #{PATHS[:bindir]} --no-rdoc --no-ri --conservative"
  BUNDLER = File.join(PATHS[:bindir], 'bundle')
  FPM = File.join(PATHS[:bindir], 'fpm')

  # fpm options that are used for both hastur-agent and hastur-server
  FPM_COMMON_OPTIONS = [
    %w[-a native -m team-tna@ooyala.com -t deb --license MIT --vendor Ooyala --depends libuuid1 --depends libffi -s dir],
    "--version", Hastur::VERSION,
    "--iteration", `lsb_release -c`.strip.split(/:\s+/)[1].gsub(/\W/, '+') || "unknown"
  ].flatten

  PROJECT_TOP = Rake.application.find_rakefile_location[1]

  HOME_DOWNLOADS = File.join ENV["HOME"], "Downloads"
  TGZ_CACHE = File.exists?(HOME_DOWNLOADS) ? HOME_DOWNLOADS : PATHS[:build]

  dirs = {} # filled in when the tarballs are opened up, or with find_dirs

  # make sure the compiler really knows what size to compile for
  case `uname -m`.chomp # linux32 fakes uname response
  when /\Ai\d86\Z/
    archflag = "-m32"
  when /\Ax86_64\Z/
    archflag = "-m64"
  else
    abort "Could not determine architecture to set -m32 / -m64. Check 'uname -m'."
  end

  # Linux only for now
  # For OSX CC will probably need to be forced to gcc and rpath stuff probably doesn't work
  # Set rpath on build / link to make sure /opt/hastur/lib works even if LD_LIBRARY_PATH isn't set
  # :bindir at the front of PATH is pretty important until rewrite_shebangs runs, don't remove it!
  BUILD_ENV = {
    "PATH"            => "#{PATHS[:bindir]}:/opt/local/bin:/bin:/usr/bin:/usr/local/bin",
    "LDFLAGS"         => "-Wl,-rpath -Wl,#{PATHS[:libdir]} -L#{PATHS[:libdir]}",
    "CPPFLAGS"        => "-I#{PATHS[:incdir]} #{archflag}",
    "CFLAGS"          => "-O2 -mtune=generic -pipe #{archflag}",
    "LIBRPATH"        => PATHS[:libdir], # for openssl, others probably ignore it
    "PKG_CONFIG_PATH" => "#{PATHS[:libdir]}/pkgconfig", # to find libffi
  }

  CONFIGURE = ["--prefix=#{PATHS[:prefix]}"]

  def download(url, md5, path)
    # write to md5.tar.gz as a cache so every run doesn't re-download
    cache = File.join(path, "#{md5}.tar.gz")
    if File.exist?(cache)
      return cache
    end

    # download if the cache isn't there
    response = HTTParty.get(url)
    if response.code.to_i >= 200 and response.code.to_i < 300
      digest = Digest::MD5.hexdigest(response.body)
      if digest == md5
        File.open(cache, "w") do |f|
          f.write response.body
        end
        return cache
      else
        abort "Download of '#{url}' failed, CHECKSUMS don't match. Expected: '#{md5}', Got: '#{digest}'"
      end
    else
      abort "Download of '#{url}' failed: #{response.code} #{response.message}"
    end
  end

  def run_required(*command)
    pid = Kernel.spawn(BUILD_ENV, *command.join(' '))
    _, status = Process.waitpid2 pid

    if status.exitstatus != 0
      if block_given?
        yield status
      else
        raise "'#{command.join(' ')}' failed."
      end
    end
  end

  # typical configure / make / make install
  def confmakeinstall(package, dir, *command)
    Dir.chdir(dir)

    run_required command

    # openssl parallel build is broken, so cheat
    if package == :openssl
      system("make -j4")  # this one will run for a bit and fail
      run_required "make" # finish up serially
    else
      run_required "make -j4"
    end

    # maybe add "make test" for packages that support it later?
    run_required "make install"
  end

  task :check_deps do
    if File.exists?("/usr/bin/apt-get")
      run_required('apt-get install  --force-yes -y -o "DPkg::Options::=--force-confold" build-essential dpkg-dev uuid-dev zlib1g-dev libssl-dev pkg-config')
    end
  end

  # make sure PATHS exist
  task :mkdirs do
    PATHS.each do |k,p| FileUtils.mkdir_p(p) end
  end

  # can be used to find pre-exploded tarballs instead of having to call download_and_untar
  task :find_dirs do
    PACKAGES.each do |pkg|
      Dir.glob("#{PATHS[:build]}/#{pkg}-[0-9]*") do |tdir|
        dirs[pkg] = tdir
      end
    end
  end

  # cleanup
  task :cleanup do
    PACKAGES.each do |pkg|
      Dir.glob("#{PATHS[:build]}/#{pkg}-[0-9]*") do |tdir|
        FileUtils.rm_rf tdir
      end
    end
  end

  # download & untar sourceballs
  task :download_and_untar do
    PACKAGES.each do |pkg|
      name="#{pkg}_tgz"
      PATHS[name.to_sym] = download(VERSIONS[pkg], CHECKSUMS[pkg], TGZ_CACHE)
      build="#{pkg}_dir"
      before = Dir.entries(PATHS[:build])
      run_required "/bin/tar -C #{PATHS[:build]} -xzvf #{PATHS[name.to_sym]}" do
        abort "Untar of #{PATHS[name.to_sym]} failed."
      end
      after = Dir.entries(PATHS[:build])
      if after.count - before.count != 1
        abort "Could not figure out directory for #{pkg} - before/after directory listings differ by more than one entry."
      end
      dirs[pkg] = File.join(PATHS[:build], Dir.entries(PATHS[:build]) - before)
    end
  end

  task :install_zlib do
    Rake::Task["hastur:find_dirs"].invoke unless dirs[:zlib]
    confmakeinstall(:zlib, dirs[:zlib], "./configure", CONFIGURE)
  end

  task :install_openssl do
    Rake::Task["hastur:find_dirs"].invoke unless dirs[:openssl]
    confmakeinstall(:openssl, dirs[:openssl], "./config", CONFIGURE,
      "--with-zlib-lib=#{PATHS[:libdir]}",
      "--with-zlib-include=#{PATHS[:incdir]}",
      "threads", "shared", "zlib-dynamic", "no-hw",
      "-L#{PATHS[:libdir]}",
      "-I#{PATHS[:incdir]}",
    )
  end

  task :install_yaml do
    Rake::Task["hastur:find_dirs"].invoke unless dirs[:yaml]
    confmakeinstall(:yaml, dirs[:yaml], "./configure", CONFIGURE)
  end

  task :install_libffi do
    Rake::Task["hastur:find_dirs"].invoke unless dirs[:libffi]
    confmakeinstall(:libffi, dirs[:libffi], "./configure", CONFIGURE, "--enable-portable-binary")
  end

  task :install_ruby do
    Rake::Task["hastur:find_dirs"].invoke unless dirs[:ruby]

    confmakeinstall(:ruby, dirs[:ruby], "./configure", CONFIGURE,
      "--with-opt-dir=#{PATHS[:libdir]}",
      "--enable-shared",
      "--disable-install-doc",
    )
  end

  task :install_gems do
    run_required GEM_INSTALL, "rake"
    run_required GEM_INSTALL, "bundler"
    run_required GEM_INSTALL, "fpm"
    run_required GEM_INSTALL, "bluepill"

    # gems with native extensions
    run_required GEM_INSTALL, "ffi"
    run_required GEM_INSTALL, "msgpack"
    run_required GEM_INSTALL, "yajl-ruby"
    run_required GEM_INSTALL, "thrift_client"
    run_required GEM_INSTALL, "redcarpet"
    run_required GEM_INSTALL, "msgpack"

    Dir.chdir PROJECT_TOP
    run_required BUNDLER, "install" do
      abort "bundle install failed"
    end
  end

  task :install_zeromq do
    Rake::Task["hastur:find_dirs"].invoke unless dirs[:zeromq]
    confmakeinstall(:zeromq, dirs[:zeromq], "./configure", CONFIGURE,
      "--without-documentation",
      "--with-pgm=no"
    )
  end

  task :setup do
    Rake::Task["hastur:mkdirs"].invoke
    Rake::Task["hastur:cleanup"].invoke
    Rake::Task["hastur:download_and_untar"].invoke
  end

  task :rewrite_shebangs do
    moves = {}
    Dir.foreach(PATHS[:bindir]) do |file|
      next unless File.file? file
      # read the first line
      File.open(file, "r") do |io|
        bang = io.readline
        # looks like ruby
        if bang =~ /\A#!.*ruby/
          moves[file] = "#{file}.new"
          # write out a new file with the new shebang
          File.open(moves[file], "w") do |out|
            out.puts "#!#{File.join(PATHS[:bindir], 'ruby')}"
            io.each_line do |line|
              out.print line
            end
          end
        end
      end
    end

    # move the new files in place
    moves.each do |old,new|
      FileUtils.mv new, old
    end
  end

  # remove only stuff that's safe to remove and keep developing
  task :safe_strip do
    %w[share ssl bin/c_rehash bin/sample_forking_server].each do |target|
      FileUtils.rm_rf File.join(PATHS[:prefix], target)
    end

    Dir.glob("#{PATHS[:libdir]}/*.a") do |static_lib|
      FileUtils.rm_f static_lib
    end

    Dir.glob("#{PATHS[:libdir]}/*.la") do |static_lib|
      FileUtils.rm_f static_lib
    end

    # TODO: strip binaries, will do later, since we may want them in early releases
  end

  # this will break further builds against /opt/hastur
  task :strip_build do
    %w[include lib/engines lib/pkgconfig bin/testrb].each do |target|
      FileUtils.rm_rf File.join(PATHS[:prefix], target)
    end
  end

  # remove the build directory
  task :clean_build do
    FileUtils.rm_rf PATHS[:build]
  end

  # build the hastur gem and install it
  task :install_hastur do
    require "hastur-server/version"

    Dir.chdir PROJECT_TOP
    run_required "#{File.join(PATHS[:bindir], 'rake')} build"

    gemfile = File.join(PROJECT_TOP, "pkg", "hastur-server-#{Hastur::VERSION}.gem")
    unless File.exists? gemfile
      abort "build failed or paths are not correct: could not find #{gemfile}"
    end

    run_required GEM_INSTALL, gemfile do
      abort "Installation of hastur-server gem '#{gemfile}' failed."
    end
  end

  task :build do
    Rake::Task["hastur:install_zlib"].invoke
    Rake::Task["hastur:install_openssl"].invoke
    Rake::Task["hastur:install_yaml"].invoke
    Rake::Task["hastur:install_libffi"].invoke
    Rake::Task["hastur:install_ruby"].invoke
    Rake::Task["hastur:install_zeromq"].invoke
  end

  task :fpm_hastur_server do
    Rake::Task["hastur:clean_build"].invoke
    Rake::Task["hastur:setup"].invoke
    Rake::Task["hastur:build"].invoke
    Rake::Task["hastur:install_gems"].invoke
    Rake::Task["hastur:install_hastur"].invoke
    Rake::Task["hastur:rewrite_shebangs"].invoke
    FileUtils.rm_rf PATHS[:build] # remove source directories

    command = [FPM_COMMON_OPTIONS,
      "--name",          "hastur-server",
      "--provides",      "hastur-server",
      "--replaces",      "hastur-agent",
    ].flatten

    run_required FPM, command, PATHS[:prefix] do
      abort "hastur-server FPM package build failed."
    end
  end

  task :fpm_hastur_agent do
    Rake::Task["hastur:clean_build"].invoke
    Rake::Task["hastur:setup"].invoke
    Rake::Task["hastur:build"].invoke
    Rake::Task["hastur:safe_strip"].invoke
    Rake::Task["hastur:install_gems"].invoke
    Rake::Task["hastur:install_hastur"].invoke
    Rake::Task["hastur:strip_build"].invoke
    Rake::Task["hastur:rewrite_shebangs"].invoke
    FileUtils.rm_rf PATHS[:build] # remove source directories

    command = [FPM_COMMON_OPTIONS,
      "--name",          "hastur-agent",
      "--provides",      "hastur-agent",
      "--conflicts",     "hastur-server",
      "--after-install", File.join(PROJECT_TOP, 'build', 'scripts', 'after-install.sh'),
    ].flatten

    run_required FPM, command, PATHS[:prefix] do
      abort "hastur-agent FPM package build failed."
    end
  end
end
