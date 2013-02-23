#!/usr/bin/perl

# PERL MODULE
use DBI;
use DBD::mysql;
use DBI qw(:sql_types);
use IO::File;

my $dbh = DBI->connect('DBI:mysql:stock;host=localhost', 'root', 'M1ng@2011',
	       { RaiseError => 1,
	       	 AutoCommit=>0
	       }
	     ) || die "Database connection not made: $DBI:: errstr";


my $sql_stockprice_after_next_earning = qq{
	SELECT close from stock.pricehistory where ticker in (?) and DAY = STR_TO_DATE(?, '%d-%b-%y');
};

my $sql_stockprice_after_last_earning = qq{
	SELECT close from stock.pricehistory where ticker in (?) and DAY = STR_TO_DATE(?, '%d-%b-%y');
};

my $sql_spyprice_after_next_earning = qq{
	SELECT close from stock.pricehistory where ticker in (?) and DAY = STR_TO_DATE(?, '%d-%b-%y');
}; 

my $sql_spyprice_after_last_earning = qq{
	SELECT close from stock.pricehistory where ticker in (?) and DAY = STR_TO_DATE(?, '%d-%b-%y');
};

my @ticker_array = qw(AAPL GE SKS GS JPM C F GOOG);


foreach my $ticker (@ticker_array) {

	my $stock_last_close = get_close_price ($sql_stockprice_after_last_earning, $dbh, $ticker, '18-oct-12');

	my $stock_next_close = get_close_price ($sql_stockprice_after_next_earning, $dbh, $ticker, '22-Jan-13');

	my $spy_last_close = get_close_price ($sql_spyprice_after_last_earning, $dbh, 'SPY', '18-oct-12');

	my $spy_next_close = get_close_price ($sql_spyprice_after_next_earning, $dbh, 'SPY', '22-Jan-13');

	my $stock_beat_ratio = ($stock_next_close-$stock_last_close)/$stock_last_close;

	my $spy_beat_ratio = ($spy_next_close-$spy_last_close)/$spy_last_close;

	my $beat_value;

	if($stock_beat_ratio > $spy_beat_ratio) {
		$beat_value =1;
	} else {
		$beat_value =0;
	}

	print "$ticker beat_value = $beat_value";
}

$dbh->disconnect();
	           
sub get_close_price {
	my $sql_price = shift;
	my $dbh = shift;
	my $ticker = shift;
	my $date = shift;
     
	my $sth = $dbh->prepare($sql_price);
	$sth->bind_param(1, $ticker);
	$sth->bind_param(2, $date);

	$sth->execute() or die "Unable to execute query";

	my $close;
	$sth->bind_columns( \$close );

	$sth->fetch or die;
	print "Finished $ticker Query $sql_price \n";
	print "$ticker close_price =$close \n";
    $sth->finish();
	return $close;
}


#$read_fh = IO::File->new("sp500.csv",'r');
#
#read_text($read_fh);
#@lines;
#sub read_text
#{
#        local $read_fh = shift;
#        
#        @lines = <$read_fh>;
#        
#}
#
#
# $dbh = DBI->connect('DBI:mysql:stock;host=localhost', 'root', 'M1ng@2011',
#	            { RaiseError => 1 }
#	           );
# for my $tick (@lines) 
# {	           
#     
#     print $tick;     
#     my $sql = qq(select competitor from stock.competitor where ticker = ?);
#	 $sth = $dbh->prepare($sql);
#	 $sth->execute(chomp($tick));
#	 my @row;
#	 while (@row = $sth->fetchrow_array()) {
#	    print "@row\n";
#	 }
#	        
#	$sth->finish();
# }	
#$dbh->disconnect();

