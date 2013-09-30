#!/usr/bin/perl
#######################################
#
# Mysql table export v 1.0.3 (2012) 
#
# Author Marco Tusa 
# Copyright (C) 2001-2003, 2012
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#######################################


use visualization;
use commonfunctions;
use mysqldbcommon;
use ConfigIniSimple;

use DirHandle;

use strict;
use warnings;

#use Tk::DialogBox;

# $dialog = $main->DialogBox( -title   => "Register This Program", -buttons => [ "Register", "Cancel" ] );


use Time::Local;
use Time::HiRes qw(gettimeofday tv_interval);

use DBI;
use Getopt::Long;
#use Thread;


$Getopt::Long::ignorecase = 0;


my $html = 0;
my $Param = {};
my $user = '';
my $pass = '';
my $help = '';
my $host = '' ;
my $outfile;
my $genericStatus = 2;
my $finalreport = '';
my @excludelist  ;
my @includelist ;
my @authorizedList ;
my $dsn;
my $conf = new ConfigIniSimple;
my $defaultfile;


$Param->{user}       = '';
$Param->{password}   = '';
$Param->{host}       = '';
$Param->{port}       = 3306;
$Param->{debug}      = 1;
$Param->{excludelist}  = '';
$Param->{includelist} = '';
$Param->{reset}       = 0;
$Param->{batch}       = 0;
$Param->{mode}       = 'exp';

$Param->{server_role}       = 'master';

my $invalidator  = 0 ;

$Param->{appheader}.="\t=============================================================================\n";
$Param->{appheader}.="\t*****************************************************************************\n";
$Param->{appheader}.="\t\t\t\Export Table MySQL \n";
$Param->{appheader}.="\t*****************************************************************************\n";
$Param->{appheader}.="\t=============================================================================\n";

$Param->{mysqlbase}=undef;
#$Param->{outfile};

################################
#INIZIALIZATION SECTION [START]#
################################
sub init()
{
    if(defined($defaultfile) && $defaultfile ne '' && $Param->{reset} == 0)
    {
        resetSettings($Param);
        
        $conf = loadIniObject($defaultfile);
        my $selected = loadSettingsFromIni($conf,$Param);


        if(defined($selected))
        {
        
            $host = defined($selected->{host})?$selected->{host}:'';
            $Param->{host} = defined($selected->{host})?$selected->{host}:'';
            $Param->{port} = defined($selected->{port})?$selected->{port}:'3306';
            $Param->{user} = defined($selected->{user})?$selected->{user}:'';
            $Param->{outfile} = defined($selected->{outfile})?$selected->{outfile}:undef;
            $outfile = defined($selected->{outfile})?$selected->{outfile}:'';
            $Param->{excludelist} = defined($selected->{excludelist})?$selected->{excludelist}:undef;
            $Param->{includelist} = defined($selected->{includelist})?$selected->{includelist}:undef;
            $Param->{invalidator} = 0;
            $Param->{batch} = defined($selected->{batch})?$selected->{batch}:0;
            $Param->{mysqlbase} = defined($selected->{mysqlbase})?$selected->{mysqlbase}:undef; 
            $Param->{mode} = defined($selected->{mode})?$selected->{mode}:'exp';
            $Param->{time} = defined($selected->{time})?$selected->{time}:'';
            $invalidator = 0;
            #$Param->{password} = defined($selected->{password})?$selected->{password}:'';

        #my %bloks = %{keys(%{$conf})};
        }
        
    }

    $Param->{reset} = 0;
    
    if( $host eq '' )
    {
        $host = getHost();
    }
    $Param->{host} = &URLDecode($host);
    
    if( $Param->{port} eq '' || $Param->{port} eq '3301')
    {
        $Param->{port} = getPort($Param->{port});
    }
    

    if(defined $outfile && $outfile ne '')
    {
         $Param->{outfile} = URLDecode($outfile);
    }
    else
    {
        $outfile = promptUser("Please insert valid path and directory  name to use for output data","/tmp/","/tmp/");
         $Param->{outfile} = URLDecode($outfile);
         #open FILEOUT, '>', $Param->{outfile} or die "Couldn't open $Param->{outfile} for writing: $!\n";
         #close FILEOUT;
    }
    
    $dsn  = "DBI:mysql:host=$Param->{host};port=$Param->{port}";
    if(defined $Param->{user} && $Param->{user} ne ''){
            $user = "$Param->{user}";
    }
    else
    {
            $user = getUser();
            $Param->{user} = $user;
    }
    
    if(defined $Param->{password} && $Param->{password} ne ''){
            $pass = "$Param->{password}";
    }
    else
    {
            $pass = getPassword();
            $Param->{password} = $pass;
    }

    if(defined $Param->{mode} && $Param->{mode} ne ''){
    }
    else
    {
            $Param->{mode} = getMode();
    }
    
    if(defined $Param->{time} && $Param->{time} ne ''){
    }
    elsif((!defined $Param->{time} || $Param->{time} eq '') && $Param->{mode} eq 'imp')
    {
            $Param->{time} = getTime();
    }
   
    
    
    #
    #if( defined $Param->{outfile}){
    #
    #    if (open FILEOUT, '>', $Param->{outfile}){
    #    }
    #}
    
    #my $filexxx;
    #
    #if( defined $Param->{outfile}){
    #
    #    if (open ($filexxx, '>', $Param->{outfile})){
    #    }
    #    print $filexxx "\n aaaaaaa \n";
    #    close $filexxx;
    #}
    
    $Param->{invalidator} = $invalidator;
    
    $Param = getExcludeList($Param);
    $Param = getIncludeList($Param);
    
    $invalidator = $Param->{invalidator};
    while($invalidator > 1 || $invalidator ==0)
    {
        $Param = checkForLists($Param);
        $invalidator = $Param->{invalidator};    
        
    }
    
    
    
}
################################
#INIZIALIZATION SECTION [ENDS]#
################################


sub getInitialOk($)
{
    $Param = shift;
    system('clear');
    
    my $reportString = $Param->{appheader};
    $reportString .= "\t=============================================================================\n";
    $reportString .= "\t\t\t\t Procedure Settings\n";
    $reportString .= "\t=============================================================================\n";
    $reportString .= "\tHost = ".$Param->{host}."\n";
    $reportString .= "\tPort = ".$Param->{port}."\n";
    $reportString .= "\tUser = ".$Param->{user}."\n";
    $reportString .= "\t------------------------\n";
    $reportString .= "\tSQL generated file = ".$Param->{outfile}."\n";
    if(defined($Param->{excludelist}) && $Param->{excludelist} ne ''){
        $reportString .= "\tExclude list = ".getArrayAsComma($Param->{excludelist})."\n";
    }
    if(defined($Param->{includelist}) && $Param->{includelist} ne ''){
        $reportString .= "\tInclude List = ".getArrayAsComma($Param->{includelist})."\n";
    }

    $reportString .= "\t=============================================================================\n";
    
    print $reportString;
    
    my $question .= "\tChoose:\n";
    $question .= "\t[1] Continue\n";
    $question .= "\t[2] Re insert all values\n\n";
    $question .= "\t[S] Save settings to file\n";
    $question .= "\t[0] EXIT\n";
    $question .= "\t=============================================================================\n";
    
    $genericStatus = promptUser($question,1,"1(default)|2|0");
    #print "\n".$genericStatus;
    if($genericStatus eq '0')
    {
        exit(0);
    }
        
    return $genericStatus;
    
}

sub resetSettings($)
{
    $Param = shift;
    $host = '';
    $Param->{host} = '';
    $Param->{port} = '3306';
    $Param->{user} = '';
    undef($Param->{outfile});
    $outfile = '';
    undef($Param->{excludelist});
    undef($Param->{includelist});
    $Param->{invalidator} = 0;
    $invalidator = 0;
    $Param->{password} = '';
    $Param->{authorized} ='root';    
    $_=0;
    close FILEOUT;
    
    return $Param;
}

################################
#Main CALL [START]#
################################

if (
    !GetOptions(
        'user|u:s'       => \$Param->{user},
        'password|p:s'   => \$Param->{password},
        'host|H:s'       => \$host,
        'port|P:i'       => \$Param->{port},
        'authorized|a:s'   => \$Param->{authorized},
        'outfile|o:s'    => \$outfile,
        'mode|m:s'    => \$Param->{mode},
        'time|t:s'    => \$Param->{time},        
        'debug|e:i'      => \$Param->{debug},
        'excludelist|x:s' => \$Param->{excludelist},
        'includelist|i:s' => \$Param->{includelist},
        'batch|b:i' => \$Param->{batch},
        'basedir:s' => \$Param->{mysqlbase},
        'help|h:s'       => \$help,
        'defaults-file:s'=>\$defaultfile,

    )
  )
{
    ShowOptions();
    exit(0);
}
else{
    init();
    while($genericStatus eq '2' && $Param->{batch} == 0)
    {
        
        $genericStatus = getInitialOk($Param);
        if($genericStatus eq '0')
        {
            exit(0);            
        }
        elsif($genericStatus eq '2')
        {
            init();
            $Param = resetSettings($Param);
            $Param->{reset} = 2;
            
        }
        elsif($genericStatus eq 'S')
        {
            my $selected = {};
            my $defaultFile = promptUser("Defaults file",$defaultfile,$defaultfile);
            my $settingsName = promptUser("Settings name ",'','');

            $selected->{host} = $Param->{host};
            $selected->{port} = $Param->{port};
            $selected->{user} = $Param->{user};
            $selected->{outfile} = $Param->{outfile};
            $selected->{excludelist} = defined($Param->{excludelist})?getArrayAsComma($Param->{excludelist}):'';
            $selected->{includelist} = defined($Param->{includelist})?getArrayAsComma($Param->{includelist}):'' ;
            $selected->{password} = $Param->{password};

            
            saveSettings($selected,$defaultFile,$settingsName);
            $genericStatus=2;
            $genericStatus = getInitialOk($Param);
            
        }
        elsif($genericStatus !=1)
        {
            $genericStatus = getInitialOk($Param);
        }
        
    }

}

if ( $help ne '' ) {
    ShowOptions();
    exit(0);
}

if($Param->{debug}){
    debugEnv();
}

#
#my $username = &promptUser("Enter the username ","AA");
#print "Name= ".$username."\n";

################################
#Main CALL [END]#
################################


##############################################################################################
# F U N C T I O N  S E C T I O N
# T O  B E C O M E S  O B J E C T  M E T H O D S
##############################################################################################
sub getMode()
{
    my $mode='';
    
    while($mode eq '' )
    {
        $mode = promptUser("Please insert valid Action MODE","exp","EXP|imp");
        if($mode ne '' && getConfirmation($mode) eq 'y')
        {
            return $mode;
        }
        else
        {
            $mode = '';
        }
    }
}

sub getTime()
{
    my $time='';
    
    while($time eq '' )
    {
        $time = promptUser("Please insert valid time stamp dd_mm_yyyy eg 23_12_2008","","dd_mm_yyyy");
        if($time ne '' && getConfirmation($time) eq 'y')
        {
            return $time;
        }
        else
        {
            $time = '';
        }
    }
    
    
}    

 
sub exportDataFromTables($)
{
    my $Param = shift;  
    my $dbh  = $Param->{dbh};
    my %databases = %{$Param->{tables}};
    my $alltables = "";
    my %locReport;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
    $mon = ++$mon;
    $year = $year+=1900;
    my $BinlogInfo;
    my $cmd;
    my $sth;
    my $time = localtime;
    my $binlog_active=0;
	

    ####################################################################
    # Prevents any write to binlog
    $dbh->do("SET SQL_LOG_BIN=0");
    ####################################################################


    ####################################################################
    # IF Slave role stop slave before doing dump
    ####################################################################
    if (defined $Param->{server_role} && $Param->{server_role} eq 'slave'){
            print "Stopping SLAVE at: $time [START]] \n";

            $cmd = "STOP SLAVE";
            $sth = $dbh->prepare($cmd);
            if ( DBI::err() ) {
                    if ( $Param->{PrintError} ) {
                        print "Error: " . DBI::errstr() . "\n";
                    }
            }
            $sth->execute();
    }
	
    
    
    $time = localtime;
    print "FLUSH NO_WRITE_TO_BINLOG TABLES WITH READ LOCK $time [START]] \n";
    $dbh->do("FLUSH NO_WRITE_TO_BINLOG TABLES WITH READ LOCK");
    $time = localtime;
    print "FLUSH NO_WRITE_TO_BINLOG TABLES WITH READ LOCK $time [END] \n";

    

    #$dbh->do("START TRANSACTION WITH CONSISTENT SNAPSHOT");
    
    #Get master log position [START]
    
            $cmd = "SHOW MASTER STATUS";
            $sth = $dbh->prepare($cmd);
            if ( DBI::err() ) {
                    if ( $Param->{PrintError} ) {
                        print "Error: " . DBI::errstr() . "\n";
                    }
            }

            $sth->execute();
            
            if (defined($sth))
            {
              my $row = undef;
              while ( $row = $sth->fetchrow_hashref() )
              {
		my $BinlogFile = $row->{'File'};
                my $BinlogPosition = $row->{'Position'};
		    if(defined $BinlogFile)
		    {
                        $BinlogInfo = "--- BINLOG INORMATION AT ($year/$mon/$mday $hour:$min:$sec ) --- \n File Name = $BinlogFile \n BinLogPosition = $BinlogPosition";
                        $binlog_active = 1;
                    }
              }
              
            }
            $sth->finish();
    #Get master log position [END]



    foreach my $key (sort keys %databases)
    {
        my @tables = @{$databases{$key}};
        my $dimTable = '';
        my $dbPath= '';
        $cmd = "use ".$key;
        $sth = $dbh->do($cmd);
        $time = localtime;
	
        my %columnHash ;
        my @columTypeDim ;
	my $tblDirty = 0;

        #if (defined($Param->{outfile})){
        #    print FILEOUT "Starting on ", $time," \n";
        #}
        print "processing = $key [start] at $time \n";
        #
        $dbPath = $Param->{outfile}.$key;
        #$dbPath .= "_".$hour."_".$min."_".$mday."_".$mon."_".$year;
        $dbPath .= "_".$mday."_".$mon."_".$year;
        
        my $cleanup = removeAllFile($dbPath, $Param->{debug});
        
        if($cleanup != 1)
        {            
            print $!;
        }
        
        print "checking for existing output directory for running database: $dbPath \n";
        if(checkDirectoryAndCreate($dbPath) == 0)
        {
            print "ERROR can't opendir $dbPath: Cannot continue \n";
        }


        if(defined $Param->{mysqlbase})
        {
            my $mysqldump = $Param->{mysqlbase}."/bin/mysqldump  -h".$Param->{host}.
            " -u".$Param->{user}.
            " -p".$Param->{password};
            if($binlog_active == 1){
                $mysqldump = $mysqldump." --master-data=2 "
            }
            #" -P".$Param->{port}." --no-data ".$key." > $dbPath/$key.sql";
            $mysqldump = $mysqldump." -P".$Param->{port}." --no-data -R --triggers ".$key." > $dbPath/$key.sql";
            print " ===========\n" . $mysqldump."\n==========\n";
            print qx($mysqldump);
        }
        
        if (defined $BinlogInfo)
        {
                if (open FILEOUT, '>', "$dbPath/$key.info"){
                    print FILEOUT "$BinlogInfo \n";
                }
                if( defined $BinlogInfo){
                   close FILEOUT or die $!;
                }
            
        }
      
        for (my $icounter = 0 ; $icounter <= $#tables; $icounter++)
        {
            my %columnHash ;
            

	    $dimTable = $tables[$icounter];
            
            
            #get table number of rows
            my $numberOfRows;
            my $currentExportingDb = $key;
            
            
            my $dimTableExp = $dimTable.".csv";#."_exp_".$hour."_".$min."_".$mday."_".$mon."_".$year.".csv";

            #Remove file if it already exists;
            unlink("${dimTableExp}");

#            print $currentExportingDb_bck . "\n";
           
            my $row = undef;
            $cmd = "SELECT SQL_NO_CACHE * INTO OUTFILE '${dbPath}/${dimTableExp}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'   FROM ${key}.${dimTable} LOCK IN SHARE MODE" ;

            #$cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";

            my $loctime = localtime;
            print "\nprocessing = $dimTable [STARTS] at $loctime \n";

            $row = $dbh->do($cmd);
            $loctime = localtime;
            print "\nprocessing = $dimTable [ENDS] at $loctime rows exported = $row \n";

        }

       
        
        $time = localtime;
        #if (defined($Param->{outfile})){
        #    print FILEOUT "\n\nClosing on", $time," \n";
        #}
        print "\n\nprocessing = $key [end] at $time \n";
        #$sth = $dbh->do($cmd);
    }
    
    #$dbh->do("COMMIT");
    $dbh->do("UNLOCK TABLES") or warn "";

    ####################################################################
    # IF Slave role stop slave before doing dump
    ####################################################################
    if (defined $Param->{server_role} && $Param->{server_role} eq 'slave'){
            print "Starting SLAVE at: $time [START]] \n";

            $dbh->{HandleError} = sub {mysqldbcommon::handle_error($dbh,1,"Slave cannot start")};
            $dbh->do("START SLAVE") or warn "Slave cannot start\n";
            if ( DBI::err() ) {
                    if ( $Param->{PrintError} ) {
                        print "Error: " . DBI::errstr() . "\n";
                    }
            }
            $dbh->{HandleError} = sub {mysqldbcommon::handle_error($dbh,0,"")};
            
    }
	
    ####################################################################
    # Prevents any write to binlog
    $dbh->do("SET SQL_LOG_BIN=1") or warn "";
    ####################################################################

    
    
    
    return \$Param;
}


sub impDataInToTables($)
{
    my $Param = shift;  
    my $dbh  = $Param->{dbh};
    my @databases = @{$Param->{databases}};
    my $alltables = "";
    my %locReport;
    my $itables = 0;
    #print " 1\n";


    foreach my $key (sort @databases)
    {
        #my @tables = @{$databases{$key}};
        my $dimTable = '';
        my $dbPath= '';
        my $cmd ="";# "use ".$key;
        my $sth ;#= $dbh->do($cmd);

        my $timeAllstart = localtime; #[gettimeofday];
        my $timeArAllstart = [gettimeofday];
        my $timeArAllend = undef;
        my $timeAllend = undef;
        my $time = undef;
        my $irows = 0;
        	
        #if (defined($Param->{outfile})){
        #    print FILEOUT "Starting on ", $time," \n";
        #}
        print "processing = $key [start] at $timeAllstart \n";
        #
        $dbPath = $Param->{outfile}.$key;
        #$dbPath .= "_".$hour."_".$min."_".$mday."_".$mon."_".$year;
        $dbPath .= "_".$Param->{time};
        
       
        print "checking for existing input directory for running database: $dbPath \n";
        if(checkDirectoryAndCreate($dbPath) == 0)
        {
            print "ERROR can't opendir $dbPath: Cannot continue \n";
            exit(1);
        }

        if(defined $Param->{mysqlbase})
        {
            my $row = undef;
            $cmd = "CREATE DATABASE IF NOT EXISTS ".$key;
            $row = $dbh->do($cmd);
            $cmd = "use ".$key;
            $row = $dbh->do($cmd);
            
            my $mysql = $Param->{mysqlbase}."/bin/mysql  -h".$Param->{host}.
            " -u".$Param->{user}.
            " -p".$Param->{password}.
            " -P".$Param->{port}." -D".$key." < $dbPath/$key.sql";
            print " ===========\n" . $mysql . "\n===========\n" ;
            no warnings 'all';
            print qx($mysql);
        }


        #my $d = new DirHandle $dbPath;
        opendir my($d), $dbPath or die "Could not open directory [$dbPath]: $!";
        
        $dbh->do('SET autocommit=0');
        $dbh->do('SET FOREIGN_KEY_CHECKS=0');
        $dbh->do('SET UNIQUE_CHECKS=0');
        #foreach $name (sort readdir(ETC)) { # list context, sorted
        #    print "$name\n"; # prints ., .., passwd, group, and so on
        #}

        #while( defined (my $file = $d->read) ) {
        no warnings 'numeric';
        foreach my $file ( sort { $a <=> $b } readdir $d ) {            
        my $tableFile ='';
        my $tableName = '';
        
        if($Param->{debug} == 1)
        {
            print "Processing  $file\n" if -T "$dbPath/$file";
        }
            next if $file =~ /^\.\.?$/;     # skip . and ..
            next if $file =~ /\.sql?$/;     # skip . and ..
            my $index = index($file,'.');
            if(index($file,'#') > 0){
                $index = index($file,'#');
                print "Processing  Chunk file $file\n";
            }
            $tableName = substr($file,0,$index);
            $tableFile = $dbPath."/".$file;        

            my $row = undef;
            $dbh->do('LOCK TABLES '.${key}.'.'.${tableName}.' WRITE');
            $dbh->do('ALTER TABLE '.${key}.'.'.${tableName}.' DISABLE KEYS');
            $cmd = "LOAD DATA INFILE '${tableFile}' IGNORE INTO TABLE ${key}.${tableName}  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'" ;



            #$cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";
            $time = localtime;
            print "\nprocessing = $tableName [start] at $time \n";
            $row = $dbh->do($cmd);
            if(defined($row))
            {
                print "Processed row(s) = ".$row;
            }
            $time = localtime;
            print "\nprocessing = $tableName [ends] at $time \n";
        
            $dbh->do('ALTER TABLE '.${key}.'.'.${tableName}.' ENABLE KEYS');            
        
            $itables++;
            if(defined($row) && $row > 0E0 )
            {
                $irows = $irows + $row;
            }    
            
        }
        #$dbh->do('SET autocommit=0');
        $dbh->do('COMMIT');
        $dbh->do('SET UNIQUE_CHECKS=1');
        $dbh->do('SET FOREIGN_KEY_CHECKS=1');
        $dbh->do('UNLOCK TABLES ');
        
            
        $timeAllend = localtime;#
        $timeArAllend = [gettimeofday];
        my $totaltime = tv_interval($timeArAllstart,$timeArAllend);
        #if (defined($Param->{outfile})){
        #    print FILEOUT "\n\nClosing on", $time," \n";
        #}
        print "\n\nprocessing = $key [end] at $timeAllend \n";

        print "======== SUMMARY ===============\n";
        print "processed tables = ".$itables."\n";
        print "processed rows = ".$irows."\n";
        print "total time = ".$totaltime."\n";
       if($irows > 0)
       {
            print "time per row = ".($totaltime/$irows)."\n";
        }


        #$sth = $dbh->do($cmd);
    }
        
    return \$Param;
}







sub ShowOptions {
    print <<EOF;
Usage: export.pl
       user|u
       password|p
       host|H
       port|P
       outfile|o
       help|h
       batch|b
       time|t
       mode|m
       excludelist|x
       includelist|i

export.pl -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -t=23_12_2008 -i=world,test -o=/home/mysql/backups/ --basedir=/usr/local/mysql

---------
EXPORT
---------
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -i=employees,sakila,world -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=exp
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -x=performance_schema,information_schema,mysql -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=exp

---------
Import 
---------
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -t=3_12_2011 -i=employees,sakila,world -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=imp



--help, -h
    Display this help message


--host=HOSTNAME, -H=HOSTNAME
    Connect to the MySQL server on the given host
--user=USERNAME, -u=USERNAME
    The MySQL username to use when connecting to the server
--password=PASSWORD, -p=PASSWORD
    The password to use when connecting to the server
--port=PORT, -P=PORT
    The socket file to use when connecting to the server
--includelist|i comma separated list of databases
--exludelist|x comma separeted list of databases
--outfile=FULLPATH, -o=FULLPATH
    Directory for backup files
--batch|b  if it has to skip any confirmation default [0]
    values 0 = disable (ask)
           1 = enable (don't ask)
--time|t This is the time stamp to append to the database name format is dd_mm_yyyy eg -t=23_12_2008
--mode|m The mode to operate [exp | imp] default is [exp]
  

EOF
}
################################
#INVOCATION SECTION     [START]#
################################



#my $dir = "/tmp";
#    print "Text files in $dir are:\n";
#    opendir(BIN, $dir) or die "Can't open $dir: $!";
#    while( defined (my $file = readdir BIN) ) {
#        print "$file\n" if -T "$dir/$file";
#    }
#    closedir(BIN);




 my %databaseTablesMap;
 my %reports;
 my $dbh = get_connection($dsn, $user, $pass);
 $Param->{dbh}=$dbh;

 $Param = getDataBases($Param);
 
 
 if($Param->{mode} eq 'exp')
 {
    $Param = getTables($Param);
    print "=== Performing Export ===\n";
    exportDataFromTables($Param);
 }
 elsif($Param->{mode} eq 'imp')
 {
    print "=== Performing Import ===\n";
    
    impDataInToTables($Param);
    
 }
 


   #my $thr = new Thread \&sub1;
   #
   # sub sub1 { 
   #     print "In the thread\n"; 
   # }

################################
#INVOCATION SECTION     [ENDS]#
################################

$Param->{dbh}->disconnect();
if( defined $Param->{outfile}){
    close FILEOUT;
}

exit(0);


