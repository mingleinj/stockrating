#!/usr/bin/perl

# PERL MODULE
use DBI;
use DBD::mysql;
use DBI qw(:sql_types);
use IO::File;

my $dbh = DBI->connect('DBI:mysql:stock;host=localhost', 'root', 'M1ng@2011',
	{ 
		RaiseError => 1,
	  	AutoCommit=>0
	}
) || die "Database connection not made: $DBI:: errstr";



printf "%d-%02d-%02d", map {$$_[5]+1900,$$_[4]+1, $$_[3]} [localtime];
#Get business day close price
my $sql_close_price = qq{
	SELECT close from stock.pricehistory where ticker in (?) and DAY = STR_TO_DATE(?, '%d-%b-%y');
};

#Get report data record id
my $sql_report_date_id = qq{
	SELECT id from stock.pricehistory where ticker in (?) and DAY = STR_TO_DATE(?, '%d-%b-%y');	
};

#Get before business day one week average close price
my $sql_oneweek_avg_before_report_price = qq{
	SELECT avg(close) from stock.pricehistory where id>? and id < (?+6);
};

#Get after business day one week average close price
my $sql_oneweek_avg_after_report_price = qq{
	SELECT avg(close) from stock.pricehistory where id<? and id > (?-6);
};

#Get after the business day close price

my $sql_after_business_day_close_price = qq{
	SELECT close from stock.Pricehistory where id =
	(select id from stock.Pricehistory where ticker in (?) and day = str_to_date(?, '%d-%b-%y'))-1};

#Get before the business day close price
my $sql_before_business_day_close_price = qq{
	SELECT close from stock.Pricehistory where id =
	(select id from stock.Pricehistory where ticker in (?) and day = str_to_date(?, '%d-%b-%y'))+1};

#Get two latest quarter report date
my $sql_getReportDate = qq{
	SELECT reportday from stock.earningsuperise where ticker in (?) order by str_to_date(reportday, '%d-%b-%y') desc limit 0,2;
};

#Get earlier two quarter report date
my $sql_getEarlierReportDate = qq{
	SELECT reportday from stock.earningsuperise where ticker in (?) order by str_to_date(reportday, '%d-%b-%y') desc limit 1,2;
};

#Get latest report earning surprise
my $sql_getLatestEarningSurprise = qq{
	SELECT superisepercent from stock.earningsuperise where ticker in (?) order by id asc limit 0,1;
};

#Get gross margin change over the last quarter
#my $sql_getGrossMargin = qq{
# select grossmagin, depreciation, totalrevenue from stock.hubreport where ticker in (?) order by id asc limit 4,5;
#};
my $sql_getGrossMargin = qq{
	select grossmagin, depreciation, totalrevenue from stock.hubreport where ticker in (?) order by id asc limit ?,?;
};

#Get average gross margin over the last four quarters
#my $sql_getAvgGrossMargin = qq{
# select grossmagin, depreciation, totalrevenue from stock.hubreport where ticker in (?) order by id asc limit 0,4;
#};

#Get PEG
my $sql_peg = qq{
	select peg from stock.m_valuation where ticker in(?);
};

#Get competitors
my $sql_competitors =qq{
	select distinct competitor from stock.competitor where ticker in (?);
};

#Get last close price
my $sql_last_close_price = qq{
	select close from stock.Pricehistory where ticker in (?) order by id asc limit 0,1
};

#Get Enterprise Value
my $sql_enterprise_value = qq{
	select enterprisevalue from stock.key_financial where ticker in (?)
};

#Get Cash Flow
my $sql_cash_flow = qq{
	select cashflow from stock.hubreport where ticker in (?) order by id asc limit 0,1
};

#Get ticker sector
my $sql_sector = qq{
	select sector from stock.industrymapping where ticker in (?)
} ;

my ($sql_peg_of_peers);
 

my ($sector, $stock_next_report_close, $next_report_date, $last_report_date, $stock_last_report_close,
 $spy_next_report_close, $spy_last_report_close, $stock_after_next_close, $stock_before_next_close,
 $earning_surprise, @gross_margin_ratio, $avg_magin_ratio, $margin_trend, $cur_revenue_growth,
 $avg_revenue_growth, $revenue_growth_trend, @array_totalrevenue, $peg, @competitors, $peg_avg_competitors,
 $cur_market_performance, $stock_last_close, $spy_last_close, $stock_enterprise_value, $stock_cash_flow,
 @array_peers_enterprise_value, @array_peers_cash_flow, $stock_enterprise_cash_ratio, @array_competitors_enterprise_cash_ratio,
 $avg_competitors_enterprise_cash_ratio, $stock_competitors_enterprise_cash_ratio);

my @ticker_report_date = ();

my @ticker_list = access_file_to_array('sp4571.txt');

#my @ticker_list = qw(AAPL GE SKS GS);
#my @ticker_list = qw(GKK);
#my @ticker_list = qw(XL DIA QQQ);
#my @ticker_list = qw(ABIO);

foreach my $ticker (@ticker_list) {

my @stock_report_date = get_array_with_one_variable($sql_getReportDate, $dbh, $ticker);

if (@stock_report_date!=3) {
	$stock_report_date[1] ='N/A';
	$stock_report_date[2] ='N/A';
}

	push (@ticker_report_date, @stock_report_date);


}

print "@ticker_report_date \n";

#open (my $file, '>', 'LogisticInput.csv');
open (my $file, '>', 'LogisticInput_04172013.csv');

print_to($file, 'Ticker', 'Sector', 'Latest report date', 'Last report date', 'Market beat value', 'Market reaction', 'Weekly Market Reaction', 'Earning surprise', 'Gross margin change', 'Margin trend', 'Revenue growth trend', 'Peg', 'Peg/Peg of peers', 'Peg/Peg Sector', 'CAPE', 'Current market performance', 'Enterprise/Cash', 'Enterprise/Cash compareed with peers');	

my @ticker_array = @ticker_report_date;

for my $i (0 .. $#ticker_list) {
	print "ticker name = $ticker_array[3*$i] \n";	
    $sector = access_with_one_variable($sql_sector, $dbh, $ticker_array[3*$i]);
    print "ticker sector = $sector \n";

	if (defined $ticker_array[3*$i+1] and defined $ticker_array[3*$i+2] and $ticker_array[3*$i+1] ne "" and $ticker_array[3*$i+2] ne "") {
		chop($ticker_array[3*$i+1]);
		chop($ticker_array[3*$i+1]);
		print "ticker date = $ticker_array[3*$i+1] \n";
		chop($ticker_array[3*$i+2]);
		chop($ticker_array[3*$i+2]);
		print "ticker date = $ticker_array[3*$i+2] \n";
	}	

    my $stock_report_id = get_close_price($sql_report_date_id, $dbh, $ticker_array[3*$i], $ticker_array[3*$i+1]);
    
    my $stock_oneweek_avg_before_report_price=get_close_price($sql_oneweek_avg_before_report_price, $dbh, $stock_report_id, $stock_report_id);
    
    my $stock_oneweek_avg_after_report_price=get_close_price($sql_oneweek_avg_after_report_price, $dbh, $stock_report_id, $stock_report_id);
    
    my $weekly_market_reaction = 'N/A';
    
	if (defined $stock_oneweek_avg_after_report_price and $stock_oneweek_avg_after_report_price ne '-999' and $stock_oneweek_avg_after_report_price ne '0' ) {
		$weekly_market_reaction = ($stock_oneweek_avg_after_report_price -$stock_oneweek_avg_before_report_price)/$stock_oneweek_avg_before_report_price;
	}

	print "$weekly_market_reaction = $weekly_market_reaction \n";
    

	$stock_next_report_close = get_close_price ($sql_close_price, $dbh, $ticker_array[3*$i], $ticker_array[3*$i+1]);

	@gross_margin_ratio = get_margin_ratio_with_one_variable($sql_getGrossMargin, $dbh, $ticker_array[3*$i]);
    print "-----gross_margin_ratio array = @gross_margin_ratio \n";	
   
    $peg = access_clean_with_one_variable($sql_peg, $dbh, $ticker_array[3*$i]);
    
    @competitors = get_array_with_one_variable($sql_competitors, $dbh, $ticker_array[3*$i]);
    
    shift(@competitors);
    print "competitors = @competitors \n";
    
    
    $sql_peg_of_peers = 'select avg(peg) from stock.m_valuation where ticker in (' . join(',',('?') x @competitors) . ')';
    if (@competitors) {
      $peg_avg_competitors = access_with_one_array($sql_peg_of_peers, $dbh, \@competitors);
    }
    
    unless ($peg_avg_competitors) {
     $peg_avg_competitors = 'N/A';
    };
    
    $stock_last_close = access_with_one_variable ($sql_last_close_price, $dbh, $ticker_array[3*$i]);
    print "normal: stock_last_close = $stock_last_close\n";
    
    $spy_last_close = access_with_one_variable ($sql_last_close_price, $dbh, 'SPY');
    print "normal: spy_last_close = $spy_last_close\n";
    
    $stock_enterprise_value = access_with_one_variable ($sql_enterprise_value, $dbh, $ticker_array[3*$i]);
    if (defined $stock_enterprise_value and $stock_enterprise_value ne "") {
     	$stock_enterprise_value =~ s/\.//g ;
     	print "normal: stock_enterprise_value1 = $stock_enterprise_value\n";
    	$stock_enterprise_value =~ s/B$/000/g;
     	print "normal: stock_enterprise_value2 = $stock_enterprise_value\n";
    	$stock_enterprise_value =~ s/M$//g;
    	print "normal: stock_enterprise_value3 = $stock_enterprise_value\n";
    }
    #if (defined$stock_cash_flow and $stock_cash_flow ne "") {
     $stock_cash_flow = access_with_one_variable ($sql_cash_flow, $dbh, $ticker_array[3*$i]);
     $stock_cash_flow =~ s/,//g;
     print "normal: stock_cash_flow = $stock_cash_flow\n";
    #}
    
    if ($stock_enterprise_value && ($stock_cash_flow!=0.0)) {
     	$stock_enterprise_cash_ratio = $stock_enterprise_value/$stock_cash_flow;
    } else {
     	$stock_enterprise_cash_ratio = 'N/A';
    }
    print "normal: stock_enterprise_cash_ratio = $stock_enterprise_cash_ratio \n";
     
    @array_peers_enterprise_value = get_array_with_one_array ($sql_enterprise_value, $dbh, \@competitors);

    @array_peers_enterprise_value = map { local $_ = $_; s/\.//g; $_ } @array_peers_enterprise_value;
    print "normal: array_peers_enterprise_value1 = @array_peers_enterprise_value \n";

    @array_peers_enterprise_value = map { local $_ = $_; s/B$/000/g; $_ } @array_peers_enterprise_value;
    print "normal: array_peers_enterprise_value2 = @array_peers_enterprise_value \n";

    @array_peers_enterprise_value = map { local $_ = $_; s/M$//g; $_} @array_peers_enterprise_value;
    print "normal: array_peers_enterprise_value3 = @array_peers_enterprise_value \n";
    
    @array_peers_cash_flow = get_array_with_one_array ($sql_cash_flow, $dbh, \@competitors);
    @array_peers_cash_flow = map { local $_ = $_; s/,//g; $_ } @array_peers_cash_flow;
    
    print "normal: array_peers_cash_flow = @array_peers_cash_flow \n";
    
    my $competitors_index;
    
    my $num= @competitors;
    
    my $total_competitors_enterprise_cash_ratio =0;
    
    my $len= 0;
    for my $index (0 .. $num-1) {
     	if (defined $array_peers_enterprise_value[$index] and defined $array_peers_cash_flow[$index] and $array_peers_cash_flow[$index] ne "" and $array_peers_cash_flow[$index]*100!=0 ) {
    		print "num=$num array_peers_enterprise_value[$index] = $array_peers_enterprise_value[$index] \n";
     		print "array_peers_cash_flow[$index] = $array_peers_cash_flow[$index] \n";
     		$len++;
     		$total_competitors_enterprise_cash_ratio = $total_competitors_enterprise_cash_ratio+ $array_peers_enterprise_value[$index]/$array_peers_cash_flow[$index];
     }
    }
    if ($len) {
     	$avg_competitors_enterprise_cash_ratio = $total_competitors_enterprise_cash_ratio/$len;
     	print "normal: avg_competitors_enterprise_cash_ratio =$avg_competitors_enterprise_cash_ratio \n";
    }else {
     	$avg_competitors_enterprise_cash_ratio =0;
    }
    
    print "stock_enterprise_cash_ratio=$stock_enterprise_cash_ratio \n";
    print "avg_competitors_enterprise_cash_ratio=$avg_competitors_enterprise_cash_ratio \n";
    
    if ($stock_enterprise_cash_ratio && $avg_competitors_enterprise_cash_ratio) {    	
     	$stock_competitors_enterprise_cash_ratio = $stock_enterprise_cash_ratio/$avg_competitors_enterprise_cash_ratio;	
    } else {
     	$stock_competitors_enterprise_cash_ratio = 'N/A';
    };
    print "normal: stock_competitors_enterprise_cash_ratio =$stock_competitors_enterprise_cash_ratio \n";
    
             
    if ($stock_next_report_close eq '-999'){
    
	     my @earlier_report_date = get_array_with_one_variable($sql_getEarlierReportDate, $dbh, $ticker_array[3*$i]);
	     my $size = scalar (@earlier_report_date);
	     print "-999: earlier_report_date = @earlier_report_date size = $size \n";
    
     if ($earlier_report_date[1]) {
	     chop($earlier_report_date[1]);
		 chop($earlier_report_date[1]);
		 $next_report_date = $earlier_report_date[1];
		 print "-999: next_report_date = $next_report_date \n";
     } else {
     	$next_report_date = 'N/A';
     }
     if ($earlier_report_date[2]) {
     	chop($earlier_report_date[2]);
		chop($earlier_report_date[2]);
		$last_report_date = $earlier_report_date[2];
    	print "-999: last_report_date = $last_report_date \n";
     } else {
     	$last_report_date = 'N/A';
     }
    
    
     if ($earlier_report_date[1]) {
     	$stock_next_report_close = get_close_price ($sql_close_price, $dbh, $ticker_array[3*$i], $earlier_report_date[1]);
     	print "-999: stock_next_report_close =$stock_next_report_close \n";
    
     	$spy_next_report_close = get_close_price ($sql_close_price, $dbh, 'SPY', $earlier_report_date[1]);
    	print "-999: spy_next_report_close =$spy_next_report_close \n";
     }
    
     $stock_last_report_close = get_close_price ($sql_close_price, $dbh, $ticker_array[3*$i], $earlier_report_date[2]);
     print "-999: stock_last_report_close =$stock_last_report_close \n";
     $spy_last_report_close = get_close_price ($sql_close_price, $dbh, 'SPY', $earlier_report_date[2]);
     print "-999: spy_last_report_close =$spy_last_report_close \n";
    
     $stock_after_next_close = get_close_price ($sql_after_business_day_close_price, $dbh, $ticker_array[3*$i], $earlier_report_date[1]);
     print "-999: stock_after_next_close =$stock_after_next_close \n";
     $stock_before_next_close = get_close_price ($sql_before_business_day_close_price, $dbh, $ticker_array[3*$i], $earlier_report_date[1]);
     print "-999: stock_before_next_close =$stock_before_next_close \n";
    
     $earning_surprise = access_clean_with_one_variable ($sql_getLatestEarningSurprise, $dbh, $ticker_array[3*$i], $earlier_report_date[1]);
     print "-999: earning_surprise =$earning_surprise \n";
    
    
    
    } else {
    
     	$next_report_date = $ticker_array[3*$i+1];
    	print "normal: next_report_date = $next_report_date \n";
     	$last_report_date = $ticker_array[3*$i+2];
    	print "normal: last_report_date = $last_report_date \n";
    
    	$stock_last_report_close = get_close_price ($sql_close_price, $dbh, $ticker_array[3*$i], $ticker_array[3*$i+2]);
        print "normal: stock_last_report_close =$stock_last_report_close \n";
		$spy_next_report_close = get_close_price ($sql_close_price, $dbh, 'SPY', $ticker_array[3*$i+1]);
        print "normal: spy_next_report_close =$spy_next_report_close \n";
		$spy_last_report_close = get_close_price ($sql_close_price, $dbh, 'SPY', $ticker_array[3*$i+2]);
    
     	$stock_after_next_close = get_close_price ($sql_after_business_day_close_price, $dbh, $ticker_array[3*$i], $ticker_array[3*$i+1]);
        print "normal: stock_after_next_close =$stock_after_next_close \n";
        
        $stock_before_next_close = get_close_price ($sql_before_business_day_close_price, $dbh, $ticker_array[3*$i], $ticker_array[3*$i+1]);
        print "normal: stock_before_next_close =$stock_before_next_close \n";
        
        $earning_surprise = access_clean_with_one_variable ($sql_getLatestEarningSurprise, $dbh, $ticker_array[3*$i]);
     	print "normal: earning_surprise =$earning_surprise \n";
    
    }
    
    my $beat_value;
    if (defined $stock_next_report_close &&defined $spy_next_report_close && defined $spy_last_report_close){
	    my $stock_beat_ratio = ($stock_next_report_close-$stock_last_report_close)/$stock_last_report_close;
	    print "$ticker_array[3*$i], stock_next_report_close=$stock_next_report_close stock_last_report_close= $stock_last_report_close stock_beat_ratio=$stock_beat_ratio \n";
		my $spy_beat_ratio = ($spy_next_report_close-$spy_last_report_close)/$spy_last_report_close;
	    print "$ticker_array[3*$i], spy_next_report_close=$spy_next_report_close spy_last_report_close= $spy_last_report_close spy_beat_ratio=$spy_beat_ratio \n";
	if($stock_beat_ratio > $spy_beat_ratio) {
		$beat_value =1;
	} else {
		$beat_value =0;
	}
    } else {
     	$beat_value ='N/A';
    }
	print "$ticker_array[3*$i] beat_value = $beat_value \n";

	my $market_reaction = 'N/A';
	if (defined $stock_after_next_close and $stock_after_next_close ne '-999') {
		$market_reaction = ($stock_after_next_close -$stock_before_next_close)/$stock_before_next_close;
	}

	print "market_reaction = $market_reaction \n";

	print"earning_surprise = $earning_surprise \n";


    print "gross_margin_ratio= $gross_margin_ratio[4] \n";
    
    #Get average gross margin over the last four quarters
    if ($gross_margin_ratio[0] ne 'N/A'&&$gross_margin_ratio[1] ne 'N/A'&&$gross_margin_ratio[2] ne 'N/A'&&$gross_margin_ratio[3] ne 'N/A'&&$gross_margin_ratio[4] ne 'N/A'){
    
     $avg_magin_ratio = ($gross_margin_ratio[0]+$gross_margin_ratio[1]+$gross_margin_ratio[2]+$gross_margin_ratio[3]+$gross_margin_ratio[4])/4;
        print ("======enter avg avg_magin_ratio=$avg_magin_ratio \n");
    } else {
     	$avg_magin_ratio ='N/A'
    }

	print "avg_magin_ratio= $avg_magin_ratio \n";

	if ($gross_margin_ratio[0] ne 'N/A' and $avg_magin_ratio ne 'N/A') {
		$margin_trend = $gross_margin_ratio[4]/$avg_magin_ratio;
	} else {
		$margin_trend ='N/A'
	}

	print "margin_trend= $margin_trend \n";

	if ($array_totalrevenue[4] ne 'N/A' && $array_totalrevenue[3] ne 'N/A' && $array_totalrevenue[0] ne 'N/A')
	{
		$cur_revenue_growth = $array_totalrevenue[4]-$array_totalrevenue[3];

		$avg_revenue_growth = ($array_totalrevenue[4] - $array_totalrevenue[0])/4;
        if ($avg_revenue_growth!=0) {
			$revenue_growth_trend =$cur_revenue_growth/$avg_revenue_growth;			
        } else {
        	$revenue_growth_trend ='N/A';
        }
	} else {

		$revenue_growth_trend ='N/A';
	}


	print "revenue_growth_trend=$revenue_growth_trend \n";

	print "competitors = @competitors \n";

	print"peg_avg_competitors=$peg_avg_competitors \n";

	if (defined $spy_last_close && defined $spy_next_report_close && ($spy_last_close!=$spy_next_report_close)){
		$cur_market_performance = ($stock_last_close -$stock_next_report_close)/($spy_last_close -$spy_next_report_close);
	} else {
		$cur_market_performance = 'N/A';
	}
	print "cur_market_performance =$cur_market_performance \n";
  	print "stock_competitors_enterprise_cash_ratio = $stock_competitors_enterprise_cash_ratio \n";
 
    print_to($file, $ticker_array[3*$i], $sector, $next_report_date, $last_report_date, $beat_value, $market_reaction, $weekly_market_reaction, $earning_surprise, $gross_margin_ratio[4], $margin_trend, $revenue_growth_trend, $peg, $peg_avg_competitors, 'N/A', 'N/A', $cur_market_performance, $stock_enterprise_cash_ratio, $stock_competitors_enterprise_cash_ratio);
    
}




close $file;

sub print_to{
	my $file = shift;
	my $ticker =shift;
	my $sector= shift;
	my $next_report_date = shift;
	my $last_report_date = shift;
	my $beat_value = shift;
	my $market_reaction = shift;
	my $weekly_market_reaction = shift;
	my $gross_margin_ratio = shift;
	my $margin_trend =shift;
	my $earning_surprise = shift;
	my $revenue_growth_trend = shift;
	my $peg = shift;
	my $peg_avg_competitors = shift;
	my $peg_to_sector_ratio =shift;
	my $cape_performance =shift;
	my $cur_market_performance = shift;
	my $stock_enterprise_cash_ratio =shift;
	my $stock_competitors_enterprise_cash_ratio = shift;
	my @row = ($ticker, $sector, $next_report_date, $last_report_date, $beat_value, $market_reaction, $weekly_market_reaction, $earning_surprise, $gross_margin_ratio, $margin_trend, $revenue_growth_trend, $peg, $peg_avg_competitors, $peg_to_sector_ratio, $cape_performance, $cur_market_performance, $stock_enterprise_cash_ratio, $stock_competitors_enterprise_cash_ratio);
	my $ticker_row = join(',',@row);
	print $file $ticker_row ."\n";	

}


# $dbh->disconnect();

sub get_close_price {
	my $sql_price = shift;
	my $dbh = shift;
	my $ticker = shift;
	my $date = shift;
	
	#print "$sql_price \n";
	#print "$ticker \n";
	#print "$date \n";
	   
	my $sth = $dbh->prepare($sql_price);
	$sth->bind_param(1, $ticker);
	$sth->bind_param(2, $date);
	
	$sth->execute() or die "Unable to execute query";
	
	my $close;
	$sth->bind_columns( \$close );
	
	#$sth->fetch or die;
	my $count = 0;
	while (my $row = $sth->fetchrow_arrayref()) {
	    $count++;
	   
	} unless ($count) {
	
	$close = '-999';
	}
	
	#print "$close \n";
	$sth->finish();
	return $close;
}

sub access_clean_with_one_variable {
my $sql_parameter = shift;
my $dbh = shift;
my $ticker = shift;
     
my $sth = $dbh->prepare($sql_parameter);
$sth->bind_param(1, $ticker);

$sth->execute() or die "Unable to execute query";

my $parameter;
$sth->bind_columns( \$parameter );

#$sth->fetch or die;
my $count = 0;
while (my $row = $sth->fetchrow_arrayref()) {
    $count++;
    chop($parameter);
    chop($parameter);
    #print "In while block count=$count parameter=$parameter \n";
   
} unless ($count) {

$parameter = 'N/A';
     #print "In unless count=$count parameter=$parameter \n";
    }


    $sth->finish();
return $parameter;
}

sub access_with_one_variable {
my $sql_parameter = shift;
my $dbh = shift;
my $ticker = shift;
     
my $sth = $dbh->prepare($sql_parameter);
$sth->bind_param(1, $ticker);
#print "$sql_parameter\n";
#print "$ticker\n";

$sth->execute() or die "Unable to execute query";

my $parameter;
$sth->bind_columns( \$parameter );

#$sth->fetch or die;
my $count = 0;
while (my $row = $sth->fetchrow_arrayref()) {
    $count++;
} unless ($count) {	
	$parameter = '-999';
    }
#print "$parameter\n";

    $sth->finish();
return $parameter;
}

sub access_with_one_array {
my $sql_parameter = shift;
my $dbh = shift;
my $array_parameter = shift;

my @array_parameter = @{$array_parameter};
     
my $sth = $dbh->prepare($sql_parameter);

    my $index;
    
    my $size= @array_parameter;
   
    for my $index (1 .. $size) {
     #print "index =$index lengeth of array_parameter=$size $array_parameter[$index-1] \n" ;
     $sth->bind_param($index, $array_parameter[$index-1]);
    
     #print "bind $index $array_parameter[$index-1] \n";
    }
    
$sth->execute( ) or die "Unable to execute query";
    
   
my $parameter;
$sth->bind_columns( \$parameter );

my $count = 0;
while (my $row = $sth->fetchrow_arrayref()) {
    $count++;
   
} unless ($count) {

$parameter = '-999';
    }
    $sth->finish();
return $parameter;
}


sub get_array_with_one_variable {
my $sql = shift;
my $dbh = shift;
my $ticker = shift;

     
my $sth = $dbh->prepare($sql);


$sth->bind_param(1, $ticker);
$sth->execute() or die "Unable to execute query";

my ($result_array_element, @ticker_row);
@ticker_row =();
push (@ticker_row, $ticker);
$sth->bind_columns(\$result_array_element);

#$sth->fetchall_arrayref or die;
while ($sth->fetchrow_array()) {	
push (@ticker_row, $result_array_element);
#print "insert ticker_row result_array_element =$result_array_element \n";
}

    #print "$ticker ticker_row = @ticker_row \n";
    $sth->finish();
    return @ticker_row;
}

sub get_margin_ratio_with_one_variable {
my $sql = shift;
my $dbh = shift;
my $ticker = shift;

     
my $sth = $dbh->prepare($sql);


$sth->bind_param(1, $ticker);
$sth->bind_param(2, "0");
$sth->bind_param(3, "5");

$sth->execute() or die "Unable to execute query";

my ($grossmargin,$depreciation, $totalrevenue, $margin_ratio, @array_margin);

@array_margin = ();
@array_totalrevenue = ();

$sth->bind_columns(\$grossmargin, \$depreciation, \$totalrevenue);

#$sth->fetchall_arrayref or die;
while ($sth->fetchrow_array()) {
$grossmargin =~ s/,//g;	
$depreciation =~ s/,//g;
$totalrevenue =~ s/,//g;
#print "========totalrevenue=$totalrevenue";
if ($totalrevenue*100) {
$margin_ratio = ($grossmargin -$depreciation)/$totalrevenue;
} else {
$margin_ratio = 'N/A';
}
#print "margin_ratio loop: $margin_ratio\n";
push(@array_margin, $margin_ratio);
push(@array_totalrevenue, $totalrevenue);
} unless($grossmargin) {
$margin_ratio = "N/A";
        @array_margin=("N/A", "N/A", "N/A", "N/A", "N/A");
} unless($totalrevenue){
@array_totalrevenue = ("N/A", "N/A", "N/A", "N/A", "N/A");
}


    #print "margin_ratio =@array_margin \n";
    $sth->finish();
    return @array_margin;
}

sub get_array_with_one_array {
my $sql_parameter = shift;
my $dbh = shift;
my $array_parameter = shift;

my @array_parameter = @{$array_parameter};
     
my $sth = $dbh->prepare($sql_parameter);

my ($result_array_element, @ticker_row);
@ticker_row =();

    my $index;
    
    my $size= @array_parameter;
   
    for my $index (1 .. $size) {
     #print "index =$index lengeth of array_parameter=$size $array_parameter[$index-1] \n" ;
     $sth->bind_param(1, $array_parameter[$index-1]);
     $sth->execute( ) or die "Unable to execute query";
     my $parameter;
$sth->bind_columns( \$result_array_element);

my $count = 0;
while (my $row = $sth->fetchrow_arrayref()) {
    $count++;
    push (@ticker_row, $result_array_element);
    #print "insert ticker_row result_array_element =$result_array_element \n";	
} unless ($count) {

$parameter = '-999';
         }
         
         #print "--------result is pushd into ticker_row:@ticker_row \n";
      }

    $sth->finish();
return @ticker_row;

}

sub access_file_to_array {
my $file = shift;
my @ticker_array=();
open my ($fh), '<', $file or die;
while ( defined( my $line = <$fh> ) ) {
chomp($line);
#print "$line------\n";
push (@ticker_array, $line);

}
return @ticker_array;

}




$dbh->disconnect();
