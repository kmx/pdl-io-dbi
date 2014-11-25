# NAME

PDL::IO::DBI - Create PDL from database (optimized for speed and large data)

# SYNOPSIS

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

# DESCRIPTION

For creating a piddle from database data one can use the following simple approach:

    use PDL;
    use DBI;
    my $dbh = DBI->connect($dsn);
    my $pdl = pdl($dbh->selectall_arrayref($sql_query));

However this approach does not scale well for large data (e.g. SQL queries resulting in millions of rows).

This module is optimized for creating piddles populated with very large database data. It currently **supports only
reading data from database** not updating/inserting to DB.

The goal of this module is to be as fast as possible. It is designed to silently converts anything into a number 
(wrong or undefined values are converted into `0`).

# FUNCTIONS

## rdbi1D

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

Items supported in `options` hash:

- type

    Defines the type of output piddles: `double`, `float`, `longlong`, `long`, `short`, `byte`.
    Default value is `auto` which means that the type of the output piddles is autodetected.

    You can set one type for all columns/piddles:

        my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, {type => double});

    or separately for each colum/piddle:

        my ($high, $low, $avg) = rdbi1D($dbh_or_dsn, $sql_query, {type => [long, double, double]});

- fetch\_chunk

    We do not try to load all query results into memory at once, we load them in chunks defined by this parameter.
    Default value is `8000` (rows).

- reshape\_inc

    As we do not try to load all query results into memory at once, we also do not know at the beginning how
    many rows there will be. Therefore we do not know how big piddle to allocate, we have to incrementally
    (re)alocated the piddle by increments defined by this parameter. Default value is `80000`.

    If you know how many rows there will be you can improve performance by setting this parameter to expected row count.

- null2bad

    Values `0` (default) or `1` - convert NULLs to BAD values (there is a performance cost when turned on).

- debug

    Values `0` (default) or `1` - turn on/off debug messages

## rdbi2D

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

Items supported in `options` hash are the same as by ["rdbi1D"](#rdbi1d).

# TODO

maybe convert DATETIME, TIMESTAMP & co. to something numerical

# SEE ALSO

[PDL](https://metacpan.org/pod/PDL), [DBI](https://metacpan.org/pod/DBI)

# LICENSE

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

# COPYRIGHT

2014+ KMX <kmx@cpan.org>
