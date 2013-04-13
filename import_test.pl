#!/usr/bin/perl -w

use strict;
#use warnings FATAL => qw( all );

use DBI;
use Data::Dumper;
use Scalar::MoreUtils qw(empty);
use DBD::mysql;
use IO::File;
use LWP::Simple;

use Log::Dispatch;
use vars qw( $LOG );

$LOG = Log::Dispatch->new(
	outputs => [
		[
			'File',
			min_level => 'debug',
			filename => 'csv_extract.log',
			mode    => '>>',
			newline   => 1
		]
	],
);

my $schema = 'root';
my $password = 'M1ng@2011';

my ($stmt, $result, $year, $month, $day);

my $dbh = DBI->connect('DBI:mysql:stock;mysql_local_infile=1;host=localhost',$schema, $password,
	       { RaiseError => 1,
	       	 AutoCommit=>1
	       }
	     ) || die "Database connection not made: $DBI:: errstr";


$LOG->info("Connected to $schema");

$stmt= "DROP TABLE IF EXISTS Pricehistory";
$result = $dbh->do($stmt) or die("Can't execute $stmt: $dbh->errstr \n");

$stmt = <<"CREATE_TABLE";

CREATE TABLE stock.Pricehistory (id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT, ticker VARCHAR(20), day DATE,  open DECIMAL(8,3), high DECIMAL(8,3), low DECIMAL(8,3), close DECIMAL(8,3), volume BIGINT, adj_close DECIMAL(8,3));
CREATE_TABLE

$result = $dbh->do( $stmt ) or die("Cannot create table: " . $dbh->errstr);

my @ticker_list = access_file_to_array('sp4571.txt');


$year = sprintf"%d", map{$$_[5]+1900}[localtime];
print "year=$year \n";
print map{$$_[5]+1900}[localtime];
print"\n";

$month = sprintf"%02d", map{$$_[4]+1}[localtime];
print "month=$month \n";

$day = sprintf"%02d", map{$$_[3]}[localtime];
print "day=$day \n";

print map{$$_[5]+1900, $$_[4]+1, $$_[3]}[localtime];
print"\n";
printf "%d-%02d-%02d", map{$$_[5]+1900, $$_[4]+1, $$_[3]}[localtime];
print"\n";


foreach my $ticker (@ticker_list) {

	my $URL='http://ichart.finance.yahoo.com/table.csv?s='.$ticker.'&d='.$month.'&e='.$day.'&f='.$year.'&g=d&a=1&b=1&c=2011&ignore=.csv';


	print "URL =$URL \n";


#	-- fetch the csv and save it as SYMBOL.csv
	my $status = getstore($URL, "$ticker.csv");
 
	if ( is_success($status) )
	{
  		print "file downloaded correctly \n";
        
        $stmt = qq{LOAD DATA LOCAL INFILE '$ticker.csv' INTO TABLE Pricehistory FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  IGNORE 1 LINES (day, open, high, low, close, volume,  adj_close) SET ticker = '$ticker'};
        
        my $sth = $dbh->prepare($stmt);
        
        $result= $sth->execute();
        
        print $result." records have been uploaded to Pricehistory \n";
        
        unlink "$ticker.csv" or die "Error deleting uploaded file: $!\n";
        

	}
	else
	{
  		print "error downloading file: $status\n";
	}


	
}

sub access_file_to_array {
	my $file = shift;
	my @ticker_array=();
	open my ($fh), '<', $file or die;
	while ( defined( my $line = <$fh> ) ) { 
		chomp($line);
		print "$line------\n";
		push (@ticker_array, $line);
		
	}
	close $file;
	return @ticker_array;
	
}

$dbh->disconnect();


	
	
	
	
	