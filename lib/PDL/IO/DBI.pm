package PDL::IO::DBI;

use strict;
use warnings;

use Exporter 'import';
our @EXPORT_OK   = qw(rdbi1D rdbi2D);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

our $VERSION = '0.004';

use constant DEBUG => $ENV{PDL_IO_DBI_DEBUG} ? 1 : 0;

use PDL;
use DBI;

my %pck = (
  byte     => "C",
  short    => "s",
  ushort   => "S",
  long     => "l",
  longlong => "q",
  float    => "f",
  double   => "d",
);

my %tmap = (
  DBI::SQL_TINYINT   => byte,        # -6
  DBI::SQL_BIGINT    => longlong,    # -5
  DBI::SQL_NUMERIC   => double,      #  2
  DBI::SQL_DECIMAL   => double,      #  3
  DBI::SQL_INTEGER   => long,        #  4
  DBI::SQL_SMALLINT  => short,       #  5
  DBI::SQL_FLOAT     => double,      #  6
  DBI::SQL_REAL      => float,       #  7
  DBI::SQL_DOUBLE    => double,      #  8
 #DBI::SQL_DATETIME  => longlong,    #  9
 #DBI::SQL_DATE      => longlong,    #  9
 #DBI::SQL_INTERVAL  => longlong,    # 10
 #DBI::SQL_TIME      => longlong,    # 10
 #DBI::SQL_TIMESTAMP => longlong,    # 11
  DBI::SQL_BOOLEAN   => byte,        # 16
 #DBI::SQL_TYPE_DATE                      91
 #DBI::SQL_TYPE_TIME                      92
 #DBI::SQL_TYPE_TIMESTAMP                 93
 #DBI::SQL_TYPE_TIME_WITH_TIMEZONE        94
 #DBI::SQL_TYPE_TIMESTAMP_WITH_TIMEZONE   95
 #DBI::SQL_INTERVAL_YEAR                 101
 #DBI::SQL_INTERVAL_MONTH                102
 #DBI::SQL_INTERVAL_DAY                  103
 #DBI::SQL_INTERVAL_HOUR                 104
 #DBI::SQL_INTERVAL_MINUTE               105
 #DBI::SQL_INTERVAL_SECOND               106
 #DBI::SQL_INTERVAL_YEAR_TO_MONTH        107
 #DBI::SQL_INTERVAL_DAY_TO_HOUR          108
 #DBI::SQL_INTERVAL_DAY_TO_MINUTE        109
 #DBI::SQL_INTERVAL_DAY_TO_SECOND        110
 #DBI::SQL_INTERVAL_HOUR_TO_MINUTE       111
 #DBI::SQL_INTERVAL_HOUR_TO_SECOND       112
 #DBI::SQL_INTERVAL_MINUTE_TO_SECOND     113
  ################## DBD::SQLite uses text values instead of numerical constants corresponding to DBI::SQL_*
  'BIGINT'           => longlong, # 8 bytes, -9223372036854775808 .. 9223372036854775807
  'INT8'             => longlong, # 8 bytes
  'INTEGER'          => long,     # 4 bytes, -2147483648 .. 2147483647
  'INT'              => long,     # 4 bytes
  'INT4'             => long,     # 4 bytes
  'MEDIUMINT'        => long,     # 3 bytes, -8388608 .. 8388607
  'SMALLINT'         => short,    # 2 bytes, -32768 .. 32767
  'INT2'             => short,    # 2 bytes
  'TINYINT'          => byte,     # 1 byte, MySQL: -128 .. 127, MSSQL+Pg: 0 to 255
  'REAL'             => float,    # 4 bytes
  'FLOAT'            => double,   # 8 bytes
  'NUMERIC'          => double,
  'DECIMAL'          => double,
  'DOUBLE'           => double,
  'DOUBLE PRECISION' => double,
  'BOOLEAN'          => byte,
  'SMALLSERIAL'      => short,    # 2 bytes, 1 to 32767
  'SERIAL'           => long,     # 4 bytes, 1 to 2147483647
  'BIGSERIAL'        => longlong, # 8 bytes, 1 to 9223372036854775807
);

# https://www.sqlite.org/datatype3.html
# http://dev.mysql.com/doc/refman/5.7/en/integer-types.html
# http://www.postgresql.org/docs/9.3/static/datatype-numeric.html
# http://msdn.microsoft.com/en-us/library/ff848794.aspx


sub rdbi1D {
  my ($dbh, $sql, $bind_values, $O) = _proc_args(@_);

  my $sth = $dbh->prepare($sql) or die "FATAL: prepare failed: " . $dbh->errstr;
  $sth->execute(@$bind_values)  or die "FATAL: execute failed: " . $sth->errstr;

  my ($c_type, $c_pack, $c_sizeof, $c_pdl, $c_bad, $c_dataref, $c_idx, $allocated, $cols) = _init_1D($sth->{TYPE}, $O);
  warn "Initial size: '$allocated'\n" if $O->{debug};
  my $null2bad = $O->{null2bad};
  my $processed = 0;

  warn "Fetching data (type=", join(',', @$c_type), ") ...\n" if $O->{debug};
  while (my $data = $sth->fetchall_arrayref(undef, $O->{fetch_chunk})) { # limiting MaxRows
    my $rows = scalar @$data;
    if ($rows > 0) {
      $processed += $rows;
      if ($allocated < $processed) {
        $allocated += $O->{reshape_inc};
        warn "Reshape to: '$allocated'\n" if $O->{debug};
        for (0..$cols-1) {
          $c_pdl->[$_]->reshape($allocated);
          $c_dataref->[$_] = $c_pdl->[$_]->get_dataref;
        }
      }
      if ($null2bad) {
        for my $tmp (@$data) {
          for (0..$cols-1) {
            unless (defined $tmp->[$_]) {
              $tmp->[$_] = $c_bad->[$_];
              $c_pdl->[$_]->badflag(1);
            }
          }
        }
      }
      for my $ci (0..$cols-1) {
        my $bytes = '';
        {
          no warnings 'pack'; # intentionally disable all pack related warnings
          no warnings 'numeric'; # disable: Argument ??? isn't numeric in pack
          no warnings 'uninitialized'; # disable: Use of uninitialized value in pack
          $bytes .= pack($c_pack->[$ci], $data->[$_][$ci]) for(0..$rows-1);
        }
        my $len = length $bytes;
        my $expected_len = $c_sizeof->[$ci] * $rows;
        die "FATAL: len mismatch $len != $expected_len" if $len != $expected_len;
        substr(${$c_dataref->[$ci]}, $c_idx->[$ci], $len) = $bytes;
        $c_idx->[$ci] += $expected_len;
      }
    }
  }
  die "FATAL: DB fetch failed: " . $sth->errstr if $sth->err;

  if ($processed != $allocated) {
    warn "Reshape to: '$processed' (final)\n" if $O->{debug};
    $c_pdl->[$_]->reshape($processed) for (0..$cols-1);
  }
  $c_pdl->[$_]->upd_data for (0..$cols-1);

  warn "rdbi1D: no data\n" unless $processed > 0;

  return @$c_pdl;
}

sub rdbi2D {
  my ($dbh, $sql, $bind_values, $O) = _proc_args(@_);

  my $sth = $dbh->prepare($sql) or die "FATAL: prepare failed: " . $dbh->errstr;
  $sth->execute(@$bind_values) or die "FATAL: execute failed: " . $sth->errstr;

  my ($c_type, $c_pack, $c_sizeof, $c_pdl, $c_bad, $c_dataref, $allocated, $cols) = _init_2D($sth->{TYPE}, $O);
  warn "Initial size: '$allocated'\n" if $O->{debug};
  my $null2bad = $O->{null2bad};
  my $processed = 0;
  my $c_idx = 0;
  my $pck = "$c_pack\[$cols\]";

  warn "Fetching data (type=$c_type) ...\n" if $O->{debug};
  while (my $data = $sth->fetchall_arrayref(undef, $O->{fetch_chunk})) { # limiting MaxRows
    my $rows = scalar @$data;
    if ($rows > 0) {
      $processed += $rows;
      if ($allocated < $processed) {
        $allocated += $O->{reshape_inc};
        warn "Reshape to: '$allocated'\n" if $O->{debug};
        $c_pdl->reshape($cols, $allocated);
        $c_dataref = $c_pdl->get_dataref;
      }
      my $bytes = '';
      if ($null2bad) {
        for my $tmp (@$data) {
          for (@$tmp) {
            unless (defined $_) {
              $_ = $c_bad;
              $c_pdl->badflag(1);
            }
          }
        }
      }
      {
          no warnings 'pack'; # intentionally disable all pack related warnings
          no warnings 'numeric'; # disable: Argument ??? isn't numeric in pack
          no warnings 'uninitialized'; # disable: Use of uninitialized value in pack
        $bytes .= pack($pck, @$_) for (@$data);
      }
      my $len = length $bytes;
      my $expected_len = $c_sizeof * $cols * $rows;
      die "FATAL: len mismatch $len != $expected_len" if $len != $expected_len;
      substr($$c_dataref, $c_idx, $len) = $bytes;
      $c_idx += $len;
    }
  }
  die "FATAL: DB fetch failed: " . $sth->errstr if $sth->err;
  if ($processed != $allocated) {
    warn "Reshape to: '$processed' (final)\n" if $O->{debug};
    $c_pdl->reshape($cols, $processed); # allocate the exact size
  }
  $c_pdl->upd_data;

  warn "rdbi2D: no data\n" unless $processed > 0;

  return $c_pdl->transpose;
}

sub _proc_args {
  my $options = ref $_[-1] eq 'HASH' ? pop : {};
  my ($dsn_or_dbh, $sql, $bind_values) = @_;

  die "FATAL: no SQL query"  unless $sql;
  die "FATAL: no DBH or DSN" unless defined $dsn_or_dbh;
  my $O = { %$options }; # make a copy

  # handle defaults for optional parameters
  $O->{fetch_chunk} =  8_000 unless defined $O->{fetch_chunk};
  $O->{reshape_inc} = 80_000 unless defined $O->{reshape_inc};
  $O->{type}        = 'auto' unless defined $O->{type};
  $O->{debug}       = DEBUG  unless defined $O->{debug};

  # reshape_inc cannot be lower than fetch_chunk
  $O->{reshape_inc} = $O->{fetch_chunk} if $O->{reshape_inc} < $O->{fetch_chunk};

  $bind_values = [] unless ref $bind_values eq 'ARRAY';

  # launch db query
  my $dbh = ref $dsn_or_dbh ? $dsn_or_dbh : DBI->connect($dsn_or_dbh) or die "FATAL: connect failed: " . $DBI::errstr;

  return ($dbh, $sql, $bind_values, $O);
}

sub _init_1D {
  my ($sql_types, $O) = @_;

  die "FATAL: no columns" unless ref $sql_types eq 'ARRAY';
  my $cols = scalar @$sql_types;
  die "FATAL: no columns" unless $cols > 0;

  my @c_type;
  my @c_pack;
  my @c_sizeof;
  my @c_pdl;
  my @c_bad;
  my @c_dataref;
  my @c_idx;

  if (ref $O->{type} eq 'ARRAY') {
    @c_type = @{$O->{type}};
  }
  else {
    $c_type[$_] = $O->{type} for (0..$cols-1);
  }

  my @detected_type = map { $sql_types->[$_] ? $tmap{$sql_types->[$_]} : undef } (0..$cols-1);
  if ($O->{debug}) {
    $detected_type[$_] or warn "column $_ has unknown type '$sql_types->[$_]' gonna use Double\n" for (0..$cols-1);
  }
  my $allocated = $O->{reshape_inc};

  for (0..$cols-1) {
    $c_type[$_] = $detected_type[$_] if !defined $c_type[$_] || $c_type[$_] eq 'auto';
    $c_type[$_] = double             if !$c_type[$_];
    $c_pack[$_] = $pck{$c_type[$_]};
    die "FATAL: invalid type '$c_type[$_]' for column $_" if !$c_pack[$_];
    $c_sizeof[$_] = length pack($c_pack[$_], 1);
    $c_pdl[$_] = zeroes($c_type[$_], $allocated);
    $c_dataref[$_] = $c_pdl[$_]->get_dataref;
    $c_bad[$_] = $c_pdl[$_]->badvalue;
    $c_idx[$_] = 0;
    my $big = PDL::Core::howbig($c_pdl[$_]->get_datatype);
    die "FATAL: column $_ mismatch (type=$c_type[$_], sizeof=$c_sizeof[$_], big=$big)" if $big != $c_sizeof[$_];
  }

  return (\@c_type, \@c_pack, \@c_sizeof, \@c_pdl, \@c_bad, \@c_dataref, \@c_idx, $allocated, $cols);
}

sub _init_2D {
  my ($sql_types, $O) = @_;

  die "FATAL: no columns" unless ref $sql_types eq 'ARRAY';
  my $cols = scalar @$sql_types;
  die "FATAL: no columns" unless $cols > 0;

  my $c_type = $O->{type};
  if (!$c_type || $c_type eq 'auto') {
    # try to guess the best type
    my @detected_type = map { $sql_types->[$_] ? $tmap{$sql_types->[$_]} : undef } (0..$cols-1);
    if ($O->{debug}) {
      $detected_type[$_] or warn "column $_ has unknown type '$sql_types->[$_]' gonna use Double\n" for (0..$cols-1);
    }
    for (0..$#detected_type) {
      my $dt = $detected_type[$_] || 'double';
      $c_type = double    if $dt eq double;
      $c_type = float     if $dt eq float    && $c_type ne double;
      $c_type = longlong  if $dt eq longlong && $c_type !~ /^(double|float)$/;
      $c_type = long      if $dt eq long     && $c_type !~ /^(double|float|longlong)$/;
      $c_type = short     if $dt eq short    && $c_type !~ /^(double|float|longlong|long)$/;
      $c_type = byte      if $dt eq byte     && $c_type !~ /^(double|float|longlong|long|short)$/;
    }
    die "FATAL: type detection failed" if !$c_type;
  }
  my $c_pack = $pck{$c_type};
  die "FATAL: invalid type '$c_type' for column $_" if !$c_pack;

  my $allocated = $O->{reshape_inc};
  my $c_sizeof = length pack($c_pack, 1);
  my $c_pdl = zeroes($c_type, $cols, $allocated);
  my $c_dataref = $c_pdl->get_dataref;
  my $c_bad = $c_pdl->badvalue;

  my $howbig = PDL::Core::howbig($c_pdl->get_datatype);
  die "FATAL: column $_ size mismatch (type=$c_type, sizeof=$c_sizeof, howbig=$howbig)" unless  $howbig == $c_sizeof;

  return ($c_type, $c_pack, $c_sizeof, $c_pdl, $c_bad, $c_dataref, $allocated, $cols);
}

1;

__END__

=head1 NAME

PDL::IO::DBI - Create PDL from database (optimized for speed and large data)

=head1 SYNOPSIS

  # simple usage - using DSN + SQL query
  my $sql = "select ymd, open, high, low, close from quote where symbol = 'AAPL' AND ymd >= 20140404 order by ymd";
  my $pdl = rdbi2D("dbi:SQLite:dbname=Quotes.db", $sql);

  # using DBI handle + SQL query with binded values
  my $dbh = DBI->connect("dbi:Pg:dbname=QDB;host=localhost", 'username', 'password');
  my $sql = "select ymd, open, high, low, close from quote where symbol = ? AND ymd >= ? order by ymd";
  # rdbi2D
  my $pdl = rdbi2D($dbh, $sql, ['AAPL', 20140104]);                     # 2D piddle
  # rdbi1D
  my ($y, $o, $h, $l, $c) = rdbi1D($dbh, $sql, ['AAPL', 20140104]);     # 5x 1D piddle (for each column)

  # using DBI handle + SQL query with binded values + extra options
  my $dbh = DBI->connect("dbi:Pg:dbname=QDB;host=localhost", 'username', 'password');
  my $sql = "select ymd, open, high, low, close from quote where symbol = ? AND ymd >= ? order by ymd";
  my $pdl = rdbi2D($dbh, $sql, ['AAPL', 20140104], { type=>float, fetch_chunk=>100000, reshape_inc=>100000 });

=head1 DESCRIPTION

For creating a piddle from database data one can use the following simple approach:

  use PDL;
  use DBI;
  my $dbh = DBI->connect($dsn);
  my $pdl = pdl($dbh->selectall_arrayref($sql_query));

However this approach does not scale well for large data (e.g. SQL queries resulting in millions of rows).

This module is optimized for creating piddles populated with very large database data. It currently B<supports only
reading data from database> not updating/inserting to DB.

The goal of this module is to be as fast as possible. It is designed to silently converts anything into a number 
(wrong or undefined values are converted into C<0>).

=head1 FUNCTIONS

=head2 rdbi1D

Queries the database and stores the data into 1D piddles.

 $sql_query = "SELECT high, low, avg FROM data where year > 2010";
 my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query);
 #or
 my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, \@sql_query_params);
 #or
 my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, \@sql_query_params, \%options);
 #or
 my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, \%options);

Example:

  my ($id, $high, $low) = rdbi2D($dbh, 'SELECT id, high, low FROM sales ORDER by id');

  # column types:
  #   id   .. INTEGER
  #   high .. NUMERIC
  #   low  .. NUMERIC

  print $id->info, "\n";
  PDL: Long D [100000]          # == 1D piddle, 100 000 rows from DB

  print $high->info, "\n";
  PDL: Double D [100000]        # == 1D piddle, 100 000 rows from DB

  print $low->info, "\n";
  PDL: Double D [100000]        # == 1D piddle, 100 000 rows from DB

Items supported in C<options> hash:

=over

=item type

Defines the type of output piddles: C<double>, C<float>, C<longlong>, C<long>, C<short>, C<byte>.
Default value is C<auto> which means that the type of the output piddles is autodetected.

You can set one type for all columns/piddles:

  my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, {type => double});

or separately for each colum/piddle:

  my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, {type => [long, double, double]});

=item fetch_chunk

We do not try to load all query results into memory at once, we load them in chunks defined by this parameter.
Default value is C<8000> (rows).

=item reshape_inc

As we do not try to load all query results into memory at once, we also do not know at the beginning how
many rows there will be. Therefore we do not know how big piddle to allocate, we have to incrementally
(re)alocated the piddle by increments defined by this parameter. Default value is C<80000>.

If you know how many rows there will be you can improve performance by setting this parameter to expected row count.

=item null2bad

Values C<0> (default) or C<1> - convert NULLs to BAD values (there is a performance cost when turned on).

=item debug

Values C<0> (default) or C<1> - turn on/off debug messages

=back

=head2 rdbi2D

Queries the database and stores the data into a 2D piddle.

  my $pdl = rdbi2D($dbh_or_dsn, $sql_query);
  #or
  my $pdl = rdbi2D($dbh_or_dsn, $sql_query, \@sql_query_params);
  #or
  my $pdl = rdbi2D($dbh_or_dsn, $sql_query, \@sql_query_params, \%options);
  #or
  my $pdl = rdbi2D($dbh_or_dsn, $sql_query, \%options);

Example:

  my $pdl = rdbi2D($dbh, 'SELECT id, high, low FROM sales ORDER by id');

  # column types:
  #   id   .. INTEGER
  #   high .. NUMERIC
  #   low  .. NUMERIC

  print $pdl->info, "\n";
  PDL: Double D [100000, 3]     # == 2D piddle, 100 000 rows from DB

Items supported in C<options> hash are the same as by L</rdbi1D>.

=head1 TODO

maybe convert DATETIME, TIMESTAMP & co. to something numerical

=head1 SEE ALSO

L<PDL>, L<DBI>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 COPYRIGHT

2014+ KMX E<lt>kmx@cpan.orgE<gt>
