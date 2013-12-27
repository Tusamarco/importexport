#!/usr/bin/perl
#######################################
#
# Mysql table export v 1.0.3 (2013) 
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

use threads ('yield','stack_size' => 64*4096,'exit' => 'threads_only','stringify');
use File::Path 'rmtree';
use Cwd qw();
use IO::Compress::Gzip qw(gzip $GzipError) ;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;
use Archive::Tar;
use visualization;
use commonfunctions;
use mysqldbcommon;
use ConfigIniSimple;

use DirHandle;

use strict;
use warnings;
use threads;
use threads::shared;
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
my $sizeByteLimit=10485760;
my $tar = 0;
my $compress = 0;
my $parallel = 1;

$Param->{user}       = '';
$Param->{password}   = '';
$Param->{host}       = '';
$Param->{port}       = 3306;
$Param->{debug}      = 0;
$Param->{excludelist}  = '';
$Param->{includelist} = '';
$Param->{reset}       = 0;
$Param->{batch}       = 0;
$Param->{mode}       = 'exp';
$Param->{sizelimit}   = $sizeByteLimit;

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
            $Param->{server_role} = defined($selected->{server_role})?$selected->{server_role}:'master';
            $invalidator = 0;
            #$Param->{password} = defined($selected->{password})?$selected->{password}:'';





        #my %bloks = %{keys(%{$conf})};
        }
        
    }
    
    if($parallel == 0){
        $parallel = 1;
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
   
    if(defined $Param->{server_role} && $Param->{server_role} ne ''){
    }
    elsif((!defined $Param->{server_role} || $Param->{server_role} eq ''))
    {
            $Param->{server_role} = getServerRole();
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
        'SizeChunckByteLimit:i'=>\$sizeByteLimit,
        'tar_output:i'=>\$tar,
        'compress_mode:i'=>\$compress,
        'multithreads_th_number:i'=>\$parallel,
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
sub getServerRole()
{
    my $serverRole='';
    
    while($serverRole eq '' )
    {
        $serverRole = promptUser("Please insert valid server role [master|slave]","","[master|slave]");
        if($serverRole ne '' && (getConfirmation($serverRole) eq 'master'||getConfirmation($serverRole) eq 'slave'))
        {
            return $serverRole;
        }
        else
        {
            $serverRole = '';
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
    my $timeAllstart = undef;
    my $timeArAllstart = undef;
    my $timeArAllend = undef;
    my $timeAllend = undef;
    my $itables =0;
    my $irows = 0;
    my $iSchema =0;
    my @schemaNames;
	

    ####################################################################
    # Prevents any write to binlog
    $dbh->do("SET SQL_LOG_BIN=0");
    ####################################################################


    ####################################################################
    # IF Slave role stop slave before doing dump
    ####################################################################
    if (defined $Param->{server_role} && $Param->{server_role} eq 'slave'){
            print "!!!!!!!!!!!!!! Stopping SLAVE at: $time [START]] !!!!!!!!!!!!!!\n";

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
    $timeAllstart = localtime; #[gettimeofday];
    $timeArAllstart = [gettimeofday];

    

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
	$iSchema++;
        push(@schemaNames,$key);
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
        my $bkup_timestamp = "_".$mday."_".$mon."_".$year;
        $dbPath .= $bkup_timestamp;
        
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
      
        #for (my $icounter = 0 ; $icounter <= $#tables; $icounter++)
        #{
            #my %columnHash ;
            my $chunks = 0 ;
            # move to a function in order to have this workign as a multi thread things.
        my %ThreadsPool;
        my $iThreads = $#tables;
        my $remainingTables = $iThreads;
        my $localParallel = $parallel;
        
        #foreach my $localTable(sort(keys(%tablesMap))){
        my @KeyTables = @tables;
        foreach (my $iCTables = 0;$iCTables< @KeyTables;){
            $dimTable = $tables[$iCTables];
            #my $localTable = $KeyTables[$iCTables];
            
ADDToPool:            
            if($remainingTables < $localParallel){
                $localParallel= $remainingTables;
            }

            for (my $iThCount = 0;$iThCount <= $localParallel; ){
                $remainingTables--;
                $dimTable = $tables[$iCTables];
                   
                $ThreadsPool{$iThCount}= threads->create(sub {return  exportTable($dimTable,$key, $dbPath, get_connection($dsn, $user, $pass))});
                $itables++;
                $iCTables++;
                $iThCount++
            }
THLoop:
            for my $keyTh (keys(%ThreadsPool)){
                my $thr = $ThreadsPool{$keyTh};
                if(defined $thr && $thr->is_joinable()){
                    my $thValue = $thr->join();
                    $irows = $irows + $thValue;
                    thr->exit() if thr->can('exit');
                    delete $ThreadsPool{$keyTh};
                }
               
                #$irows = $irows + loadTable($TableObjectLoc, get_connection($dsn, $user, $pass), $Param, $key);
            }
            #sleep 1;
            if($Param->{debug} == 1)
            {
                print "Thread running pool  ".keys(%ThreadsPool)."\n";
            }

            if(keys(%ThreadsPool) > 0 && keys(%ThreadsPool) < 2 && $localParallel > 1)
            {
                goto ADDToPool;
                
            }
            elsif(keys(%ThreadsPool) > 0){
                goto THLoop;
            }
            #else{
            #    print "\n AAAAAAAAAAAA \n"
            #    
            #}
                
        }
       for my $keyTh (keys(%ThreadsPool)){
        my $thr = $ThreadsPool{$keyTh};
            if(defined $thr && $thr->is_joinable()){
                my $thValue = $thr->join();
                thr->exit() if thr->can('exit');
                delete $ThreadsPool{$keyTh};
            }
       }

        

       if($tar > 0){
            my $tar = Archive::Tar->new;
            chdir $dbPath;
            my $path = Cwd::cwd();
            print "$path\n";

            opendir my($d), "." or die "Could not open directory [$dbPath]: $!";

            my @files = undef;
            
            foreach my $file ( readdir $d ) {
                if($file ne ".."){
                    push(@files,$file);
                }
            }
            splice @files, 0,1;
            
            foreach my $tarInfile(sort @files){
                $tar->add_files($tarInfile);
                if($compress > 1 ){
                    $tar->write("../".$key.$bkup_timestamp.".tar.gz",COMPRESS_GZIP );
                }
                else
                {
                    $tar->write("../".$key.$bkup_timestamp.".tar" );
                }
            }
            chdir "../";
            rmtree($dbPath);
            
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
            print "!!!!!!!!!!! Starting SLAVE at: $time [START]] !!!!!!!!!\n";

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

    $timeAllend = localtime;#
    $timeArAllend = [gettimeofday];
    my $totaltime = tv_interval($timeArAllstart,$timeArAllend);
    #if (defined($Param->{outfile})){
    #    print FILEOUT "\n\nClosing on", $time," \n";
    #}
    
    print "\n\n======== SUMMARY ===============\n";
    print "processed Schemas = ".$iSchema."\n";
    print "processed Schemas =  @schemaNames\n";
    print "processed tables = ".$itables."\n";
    print "processed rows = ".$irows."\n";
    print "total time = ".$totaltime."\n";
    if($irows > 0)
    {
        print "time per row = ".($totaltime/$irows)."\n";
    }    
    
    return \$Param;
}

sub exportTable($$$$){
                #get table number of rows
    my $dimTable = shift;
    my $key = shift;
    my $dbPath=shift;
    my $dbh = shift;
    my $cmd ="";
                
    my $numberOfRows;
    my $currentExportingDb = $key;
    
    
    my $dimTableExp = $dimTable.".csv";#."_exp_".$hour."_".$min."_".$mday."_".$mon."_".$year.".csv";

    #Remove file if it already exists;
    unlink("${dimTableExp}");

#            print $currentExportingDb_bck . "\n";
   
    my $rows = undef;
    $cmd = "SELECT SQL_NO_CACHE * INTO OUTFILE '${dbPath}/${dimTableExp}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'   FROM ${key}.${dimTable} LOCK IN SHARE MODE" ;

    #$cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";

    my $loctime = localtime;
    print "\nprocessing = $dimTable [STARTS] at $loctime \n";

    $rows = $dbh->do($cmd);
    $loctime = localtime;
    
    
    #getting the size info for the file
    my $filesize = -s "${dbPath}/${dimTableExp}";
    print "\nprocessing = $dimTable Has size $filesize Bytes and #rows exported = $rows \n";
    if($filesize > $Param->{sizelimit}){
        $Param->{splitfile} = "${dbPath}/${dimTable}";
        $Param->{splitrows} = $rows;
        splitFile($Param,$filesize);
        #my $thr = threads->create('splitFile', $Param,$filesize);
        #$thr->join();
#                $chunks = splitFile($Param,"${dbPath}/${dimTableExp}",$rows);
    }
    else{
        if($compress >0 && $compress < 2){
            #compress the close file and remove original 
            gzip  "${dbPath}/${dimTableExp}" => "${dbPath}/${dimTable}".".gz" or die "gzip failed: $GzipError\n";
            unlink "${dbPath}/${dimTableExp}";
            
        }
    }
    
    print "\nprocessing = $dimTable [ENDS] at $loctime rows exported = $rows \n";
    return $rows;

    
    
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
    my %tableObject;
    
    
    if($#databases < 0){
        
        print "There is no Schema/Database in the server that has a valid name for import\nas a security matter you must at least have the schema created defore importing the tables\n
        Simply execute:
        \t CREATE DATABASE <NAME>;\nFor each desired schema.\n
        ";
        
    }
    
    
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
        my %tablesMap;
        	
        #if (defined($Param->{outfile})){
        #    print FILEOUT "Starting on ", $time," \n";
        #}
        print "processing = $key [start] at $timeAllstart \n";
        #
        $dbPath = $Param->{outfile}.$key;
        #$dbPath .= "_".$hour."_".$min."_".$mday."_".$mon."_".$year;
        $dbPath .= "_".$Param->{time};
        
        my $tarfile = undef;
        
        if ($tar == 1 ){
            $tarfile = $key."_".$Param->{time}.".tar";
        }
        elsif($tar == 2){
            $tarfile = $key."_".$Param->{time}.".tar.gz";
        }
        if($tar > 0){
            #$Archive::Extract::PREFER_BIN = 1;
            
            chdir $Param->{outfile};
            my $path = Cwd::cwd();
            print "$path\n";

            my $archive =   my $tar = Archive::Tar->new;
#            $archive->read($tarfile);
            
            checkDirectoryAndCreate($dbPath);
            
            chdir $dbPath;
            $archive->extract_archive("../".$tarfile);
            #$archive->extract();
            chdir "../";
            
            #Archive::Tar->new(archive=> $tarfile,'tar');
            #$archive->extract(to=> $dbPath);
        }
       
       
       
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

        #browse the directory and identify if we have simple table or split one
        # also check for compression and define prepare for parallel execution
        no warnings 'numeric';
        foreach my $file ( sort { $a <=> $b } readdir $d ) {            
            my $tableFile ='';
            my $tableName = '';
            my $extention = '';
            my $isSplit =0;
            my $isCompress =0;
            my @TableFiles;
            
            #if($Param->{debug} == 1)
            #{
            #    print "Processing  $file\n" if -T "$dbPath/$file";
            #}
    
            next if $file =~ /^\.\.?$/;     # skip . and ..
            next if $file =~ /\.sql?$/;     # skip . and ..
            next if $file =~ /\.log?$/;     # skip . and ..
            next if $file =~ /\.info?$/;     # skip . and ..
            
            $extention = substr($file,index($file,'.')+1,length($file));
            my $index = index($file,'.');
            if(index($file,'#') > 0){
                $index = index($file,'#');
                #print "Processing  Chunk file $file\n";
            }
            $tableName = substr($file,0,$index);
            $tableFile = $dbPath."/".$file;
            $tablesMap{$tableName}{name}=$tableName;
            if( defined @{$tablesMap{$tableName}{path}}){
                @TableFiles=@{$tablesMap{$tableName}{path}};
            }
            push(@TableFiles,$tableFile);
            $tablesMap{$tableName}{path}=\@TableFiles;
            $tablesMap{$tableName}{isSplit}=index($file,'#') >0?1:0;
            $tablesMap{$tableName}{isCompress}=$extention eq "gz"?1:0;
            
            
        }

        no warnings 'numeric';
        my $iThreads = keys(%tablesMap);
        my $remainingTables = $iThreads;
        my %ThreadsPool;
        my $localParallel = $parallel;
        
        #foreach my $localTable(sort(keys(%tablesMap))){
        my @KeyTables = keys(%tablesMap);
        foreach (my $iCTables = 0;$iCTables< @KeyTables;){
            #my $localTable = $KeyTables[$iCTables];
            my %ThreadsPool;
ADDToPool:            
            if($remainingTables < $localParallel){
                $localParallel= $remainingTables;
            }

            for (my $iThCount = 1;$iThCount <= $localParallel; ){
                $remainingTables--;
                my $TableObjectLoc = $tablesMap{$KeyTables[$iCTables]};
                   
                $ThreadsPool{$iThCount}= threads->create(sub {return loadTable($TableObjectLoc, get_connection($dsn, $user, $pass), $Param, $key)});
                $itables++;
                $iCTables++;
                $iThCount++
            }
THLoop:
            for my $keyTh (keys(%ThreadsPool)){
                my $thr = $ThreadsPool{$keyTh};
                if(defined $thr && $thr->is_joinable()){
                    my $thValue = $thr->join();
                    if(defined $thValue ){
                        $irows = $irows + $thValue;
                    }
                    thr->exit() if thr->can('exit');
                    delete $ThreadsPool{$keyTh};
                }
               
                #$irows = $irows + loadTable($TableObjectLoc, get_connection($dsn, $user, $pass), $Param, $key);
            }
            sleep 2;
            if($Param->{debug} == 1)
            {
                print "Thread running pool  ".keys(%ThreadsPool)."\n";
            }

            if(keys(%ThreadsPool) > 0 && keys(%ThreadsPool) < 2 && $localParallel < 1)
            {
                goto ADDToPool;
                
            }
            elsif(keys(%ThreadsPool) > 0){
                goto THLoop;
            }
                
        }
# house keeping
        for my $keyTh (keys(%ThreadsPool)){
                my $thr = $ThreadsPool{$keyTh};
                thr->exit() if thr->can('exit');
                delete $ThreadsPool{$keyTh};
        }

        
        #$dbh->do('SET autocommit=0');
        $dbh->do('COMMIT');
        $dbh->do('SET UNIQUE_CHECKS=1');
        $dbh->do('SET FOREIGN_KEY_CHECKS=1');
        $dbh->do('UNLOCK TABLES ');
        
        if($tar > 0 ){
            rmtree($dbPath);
        }
        
            
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
            print "time per row = ".($totaltime/$irows)."\n\n\n";
        }


        #$sth = $dbh->do($cmd);
    }
        
    return \$Param;
}

sub loadTable($$$$){
    my $TableObjectLoc = shift;
    my $dbh = shift;
    my $Param = shift;
    my $key = shift;
    my @files=@{$TableObjectLoc->{path}};
    my $irows = 0;
    
    $dbh->do('SET autocommit=0');
    $dbh->do('SET FOREIGN_KEY_CHECKS=0');
    $dbh->do('SET UNIQUE_CHECKS=0');


    
    print   "Processing Table = ".$TableObjectLoc->{name}."\n";
    print   "Analizing [$key.$TableObjectLoc->{name}] with files number = ".@{$TableObjectLoc->{path}}."\n";

    foreach my $file ( sort @files ) {            
        my $tableFile ='';
        my $tableName = '';
        my $extention = "";
        
        if($Param->{debug} == 1)
        {
            print "Processing  $file\n" if -T "$file";
        }

        $extention = substr($file,index($file,'.')+1,length($file));
        if($extention eq "gz"){
            my $filedest = substr($file,0,index($file,'.')).".csv";
            gunzip  "${file}" => "${filedest}" or die "gzip failed: $GzipError\n";
            $file = $filedest;
        }
            my $index = index($file,'.');
            
            
            if(index($file,'#') > 0){
                $index = index($file,'#');
                print "Processing  Chunk file $file\n";
            }
            $tableName = substr($file,0,$index);
            $tableFile = $file;        

            my $row = undef;
            $dbh->do('LOCK TABLES '.${key}.'.'.$TableObjectLoc->{name}.' WRITE');
            $dbh->do('ALTER TABLE '.${key}.'.'.$TableObjectLoc->{name}.' DISABLE KEYS');
            if($Param->{debug} == 1){
                print "\nLOAD DATA INFILE '${tableFile}' IGNORE INTO TABLE ${key}.$TableObjectLoc->{name}  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'\n" ;     
                
            }
            
            my $cmd = "LOAD DATA INFILE '${tableFile}' IGNORE INTO TABLE ${key}.$TableObjectLoc->{name}  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'" ;



            #$cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";
            my $time = localtime;
            print "processing = ".$TableObjectLoc->{name}." [start] at $time \n";
            $row = $dbh->do($cmd);
            if(defined($row))
            {
                print "Processed row(s) = ".$row;
            }
            $time = localtime;
            print "\nprocessing = ".$TableObjectLoc->{name}." [ends] at $time \n\n";
            if($extention eq "gz"){
                unlink "${file}";
            }
            $dbh->do('ALTER TABLE '.${key}.'.'.$TableObjectLoc->{name}.' ENABLE KEYS');            
        
            #$itables++;
            if(defined($row) && $row > 0E0 )
            {
                $irows = $irows + $row;
            }
            
            $dbh->do('COMMIT');
    }
    #dbh->disconnect();
    return $irows;

}



sub splitFile($$){
    my $Param = shift;
    my $filesize = shift;
    my $thid = threads->self()->tid();
    my $PathFile = $Param->{splitfile};  
    my $Lines = $Param->{splitrows};
    my $LinesPercen =eval($Lines * 0.005);
    my $sizelimit = $Param->{sizelimit};
    my $logTableFile = $PathFile.".log";
    my $chunkSize = 0;
    my $chunkNumber = 0;
 
    $chunkSize = $sizelimit/($filesize/$Lines);
    $chunkNumber = $Lines/$chunkSize;
    my $eachBytes = eval($filesize/$Lines);
    
    printf("Thread id =%2s Number of lines =%2s Processing file =%2s   Table Process Log =%2s \n", $thid,$Lines,$PathFile.".csv",$PathFile.".log" );
    
    
    
     if (open FILELOGT, '>', $logTableFile){
                print FILELOGT sprintf("Thread id =%2s Number of lines =%2s Processing file =%2s   Table Process Log =%2s \n", $thid,$Lines,$PathFile.".csv",$PathFile.".log" );
                print FILELOGT "This File will be split in $chunkNumber\nEach chunk will have $chunkSize lines\nEach line will be of $eachBytes bytes\n";
                FILELOGT->autoflush(1);
    }

    open(MASTER, $PathFile.".csv") or die("Could not open  file.");
    
    my $count = 0;
    my $linecount = 0;
    my $GlobalLinecount = 0;
    my $chunk = 1;
    
    #open first chunk
    my $cunckFile = $PathFile."#chunk_$chunk";
    open(CHUNK, '>',$cunckFile.".csv") or die("Could not open  file.");
    
    my $startTime = localtime;
    my $endTime = localtime;
    print FILELOGT sprintf("Chunk id =%2s Starting, Start time =%2s, File =%2s \n", $chunk,$startTime,$PathFile."#chunk_$chunk.csv" );        
    #loop from the master file and split it in to chunks
    foreach my $line (<MASTER>)  {   
        print CHUNK $line;
        #increments local counters
        ++$count;
        ++$linecount;
        ++$GlobalLinecount;
        
        if($count > $chunkSize){
            #reset local chunk and push information with time and location on the table log;
            $endTime = localtime;
            print FILELOGT sprintf("Chunk id =%2s Filled, Start time =%2s End time =%2s, File =%2s \n", $chunk,$startTime,$endTime,$PathFile."#chunk_$chunk.csv" );
            
            CHUNK->autoflush(1);
            close(CHUNK);
            ++$chunk;
            
            #compress the close file and remove original 
            if($compress >0 ){
                gzip  $cunckFile.".csv" =>  $cunckFile.".gz" or die "gzip failed: $GzipError\n";
                unlink $cunckFile.".csv";
            }
            
            #open new file
            $cunckFile = $PathFile."#chunk_$chunk";
            open(CHUNK, '>', $cunckFile.".csv") or die("Could not open  file.");

            $count = 0;
            $startTime = localtime;
            print FILELOGT sprintf("Chunk id =%2s Starting, Start time =%2s, File =%2s \n", $chunk,$startTime,$PathFile."#chunk_$chunk.csv" );
        }
        if($linecount > $LinesPercen){
            my $percentdone = substr(eval(($GlobalLinecount/$Lines)*100), 0, 5);
            print FILELOGT "Processing Line = $GlobalLinecount  Processed ${percentdone}% of the total $Lines\n";;
            $linecount =0;
            
        }
        
    }
    $endTime = localtime;
    print FILELOGT sprintf("Chunk id =%2s Filled, Start time =%2s End time =%2s, File =%2s \n", $chunk,$startTime,$endTime,$PathFile."#chunk_$chunk.csv" );
    close(CHUNK);

    if($compress >0 ){
        #compress the close file and remove original 
        gzip  $cunckFile.".csv" =>  $cunckFile.".gz" or die "gzip failed: $GzipError\n";
        unlink $cunckFile.".csv";
    }
    
    #close the master file and remove it    
    close(MASTER);
    unlink $PathFile.".csv";




    close(FILELOGT);
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
       compress|c
       tar

export.pl -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -t=23_12_2008 -i=world,test -o=/home/mysql/backups/ --basedir=/usr/local/mysql

---------
EXPORT
---------
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -i=employees,sakila,world -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=exp
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -x=performance_schema,information_schema,mysql -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=exp

Use TAR and compression
-----------------------
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 --tar_output=1 --compress_mode=1 -x=performance_schema,information_schema,mysql -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=exp

---------
Import 
---------
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -t=3_12_2011 -i=employees,sakila,world -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=imp

Use TAR, compression and multi threads
-----------------------------------------
export.pl  -u=root -p=mysql -H=127.0.0.1 -P=3310 -b=1 -t=3_12_2011 --tar_output=1 --compress_mode=1 --multithreads_th_number=4 -i=employees,sakila,world -o=/tmp/ --basedir=/home/mysql/templates/mysql-55p -m=imp




--help, -h
    Display this help message
    

-------- CONNECTION PARAMETERS ----------------
--host=HOSTNAME, -H=HOSTNAME
    Connect to the MySQL server on the given host

--user=USERNAME, -u=USERNAME
    The MySQL username to use when connecting to the server

--password=PASSWORD, -p=PASSWORD
    The password to use when connecting to the server

--port=PORT, -P=PORT
    The socket file to use when connecting to the server


-------- Schema filtering PARAMETERS ----------------
--includelist|i comma separated list of databases

--exludelist|x comma separeted list of databases

--time|t This is the time stamp to append to the database name format is dd_mm_yyyy eg -t=23_12_2008


-------- Operational PARAMETERS ----------------
--outfile=FULLPATH, -o=FULLPATH
    Main root Directory for backup files

--batch|b  if it has to skip any confirmation default [0]
    values 0 = disable (ask)
           1 = enable (don't ask)

--mode|m The mode to operate
    Export for extracting the data from the server
    Import to load data in the server
    [exp | imp] default is [exp]
    
--multithreads_th_number
    Number of multiple thread to run at the same time to EXTRACT & LOAD data in to the server.
    This option is valid durign both phases Export & Import.
    Default number of thread is 1 so not parallel load.
    It is possible to set any number up to 8.
  


-------- Output handling PARAMETERS ----------------
--tar_output [0|1] Default 0
    create a tar file per exported database
--compress_mode [0|1|2] Default 0
    Using mode 1 it will compress Gzip each data file
    Using mode 2 it will compress the final TAR file 
  

EOF
}
################################
#INVOCATION SECTION     [START]#
################################



#   my $dir = "/tmp";
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

