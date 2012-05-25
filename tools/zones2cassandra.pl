#!/usr/bin/perl

=head1 NAME

zones2cassandra.pl - parse zones files and write reverse mappings to Cassandra

=head1 DESCRIPTION

We assign CNAME's to pretty much all of our hosts that are human-readable.  The hosts
rarely have these useful names consistently mapped, so it's safest to map to the CNAME
using a reverse mapping built from our zone files.

This is written in perl instead of ruby because most of the DNS side already existed
and the perl parser is more reliable.

=head1 SYNOPSIS

perl zones2cassandra.pl $CODE_ROOT/dns/zones/pri/foo.zone

=cut

use strict;
use warnings;
use Net::DNS::ZoneFile::Fast;
use File::Basename;
use Data::Dumper;
use IO::String;
use File::Slurp;
use Cassandra::Lite;
use DateTime;

our %names;

my $c = Cassandra::Lite->new(
    keyspace => "Hastur",
    server_port => 9202,
    consistency_level_write => 'TWO'
);

my $today_usec = DateTime->today->epoch * 1_000_000;

foreach my $zonefile ( @ARGV ) {
    my $path = dirname($zonefile);

    # read the data in and remove inconvenient lines like includes
    my $zone_txt = read_file($zonefile);
    $zone_txt =~ s/\$include[^\n]+\n//gi;
    my $fh = IO::String->new($zone_txt);

    my $rr = Net::DNS::ZoneFile::Fast::parse( fh => $fh );

    foreach my $r ( @$rr ) {
        if ( $r->type eq 'CNAME' ) {
            my $host = $r->rdatastr;
               $host =~ s/\.$//;

            next if exists $names{$host};
            $names{$host} = $r->name;
        }
    }
}

#$c->delete("LookupByKey", "cnames-$today_usec");
$c->put("LookupByKey", "cnames-$today_usec", \%names);

