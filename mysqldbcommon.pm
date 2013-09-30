#!/usr/bin/perl
#######################################
#
# Mysql table audit v 0.5 (2008) - BETA!!!!
#
# Author Marco Tusa 
# Copyright (C) 2001-2003, 2008
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

package mysqldbcommon;

use commonfunctions;
use visualization;
use strict;
use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);

require Exporter;
our @ISA = qw(Exporter);
@EXPORT      = qw(getDataBases getTables get_connection getTableStatus getHost getUser getPort getPassword);      

sub handle_error ($$$){
  #my $error = shift; 
  my $dbh = shift ;
  my $to_handle = shift;
  my $text = shift;
  my $handled = 0; # did we handle the error?

  #print "\n\tSub 'handle_error': $error\n";

  if ( $to_handle > 0 ) {    # if you want to say this error is not an error
    # clear the error
    $dbh->set_err(undef,undef);

    # set the new return value
    $_[2] = { $text };

    # update our handled flag
    $handled = 1;

  }
  #else {      # we acknowledge it was an error
  #  # muck around with $error here and store it in $_[0]
  #  # $_[0] is the error message seen by RaiseError/PrintError
  #  #$_[0] = 'The gremlins are loose again!';
  #
  #  # potentially change the error too
  #  #$dbh->set_err(1234, $_[0]);
  #}

}

sub get_connection($$$) {
  my $dsn  = shift;
  my $user = shift;
  my $pass = shift;

  my $dbh = DBI->connect($dsn, $user, $pass,  {RaiseError => 1, PrintError => 1});
 
  if (!defined($dbh)) {
    print  "Cannot connect to $dsn as $user\n";
    die();
  }
  
  return $dbh;
}

sub getHost()
{
    my $hostlocal = '';
    if( $hostlocal eq '' )
    {
        while($hostlocal eq '' && length($hostlocal) < 4)
        {
            $hostlocal = promptUser("Please insert valid host to connect to ","127.0.0.1","127.0.0.1");
            if($hostlocal ne '' && getConfirmation($hostlocal) eq 'y')
            {
                return $hostlocal;
            }
            else
            {
                $hostlocal = '';
            }

        }
    }
    
    
}

sub getPort($)
{
    my $Portlocal = shift;
    if( $Portlocal eq '3306' )
    {
        while($Portlocal eq '3306' || $Portlocal eq '')
        {
            $Portlocal = promptUser("Please insert a port number ","3306","3306");
            if($Portlocal ne '' && getConfirmation($Portlocal) eq 'y')
            {
                return $Portlocal;
            }
            else
            {
                $Portlocal = '';
            }

        }
    }
    
    
}


sub getUser()
{
    my $userlocal='';
    
    while($userlocal eq '' )
    {
        $userlocal = promptUser("Please insert valid user for the connection","","");
        if($userlocal ne '' && getConfirmation($userlocal) eq 'y')
        {
            return $userlocal;
        }
        else
        {
            $userlocal = '';
        }
    }
    
    
}


sub getPassword()
{
    my $passlocal='';
    
    while($passlocal eq '' )
    {
        $passlocal = promptUser("Please insert valid password for the connection","","");
        if($passlocal ne '' && getPasswordConfirmation($passlocal) eq 'y')
        {
            return $passlocal;
        }
        else
        {
            $passlocal = '';
        }
    }
    
    
}


sub getDataBases($)
{
  my $Param = shift ;  
  my $dbh  = $Param->{dbh};
  my @exclude ;
  my @include ;
  my @databases ;
  my $mode = 0;
  
  if ( defined $Param->{excludelist} ) { @exclude = @{$Param->{excludelist}} };
  if ( defined $Param->{includelist} ) { @include = @{$Param->{includelist}} };
  if ( defined $Param->{mode} && $Param->{mode} eq "imp") {
          $mode = 1;
        };
  
  my $cmd = "show databases";
  my $sth = $dbh->prepare($cmd);
  $sth->execute();
  
  my $recordSet = $sth->fetchrow_hashref();
  my $load = 1;
  
  if (defined($sth))
  {
    while ( my $row = $sth->fetchrow_hashref() )
    {
       $load = 1;		
       my $v= $row->{'Database'};
       
       if(defined $Param->{excludelist})
       {
	    for (my $i=0; $i < @exclude; $i++) {
              $load = 0 ;
		if ($exclude[$i] eq $v) {
		    $load = 1 ;
                    last;
		}
                
              #print $exclude[$i]." " . $load . " " . $v ."\n";              
	    }
            if($load == 0) {$databases[++$#databases] = $v};
	    
       }
       elsif(defined $Param->{includelist} && $mode == 0)
       {
	    for (my $i= 0; $i < @include; $i++) {
		if ($include[$i] eq $v) {
		    $load = 1;
		    #last;
		    
		}
		else
		{
		    $load = 0;    
		}
                if($load) {$databases[++$#databases] = $v};
	    }
	}
       elsif(defined $Param->{includelist} && $mode == 1)
       {
	    for (my $i= 0; $i < @include; $i++) {
		$databases[++$#databases] = $include[$i];
                
	    }
            last;
	}
       else
       {
	    $load = 1;
        }
       
       #if($load) {$databases[++$#databases] = $v};
    }

    $Param->{databases} = \@databases;
  }
  return $Param;
 }

 sub getTables($)
 {
  my $Param = shift;  
  my $dbh  = $Param->{dbh};
  my @databases = @{$Param->{databases}};

  my $boundary = $#databases;
  my %tableHashMap ;
    
  for( my $counter = 0 ; $counter <= $boundary; $counter++ )
  {
        my @tables ;
	my %tablesh ;
        my $currDb = $databases[$counter];
        if(defined $currDb && $currDb ne  "")
        {
       
            my $cmd = "use ".$currDb;
            my $sth = $dbh->prepare($cmd);
            if ( DBI::err() ) {
                    if ( $Param->{PrintError} ) {
                        print "Error: " . DBI::errstr() . "\n";
                        exit(1);
                    }
            }
    
            $sth->execute();
            $cmd = "SHOW TABLE STATUS";
            $sth = $dbh->prepare($cmd);
            $sth->execute();
            
            if (defined($sth))
            {
              my $row = undef;
              while ( $row = $sth->fetchrow_hashref() )
              {
		my $engine = $row->{'Engine'};
		    if(defined $engine)
		    {
			if ($engine eq "MyISAM" or $engine eq 'InnoDB' or $engine eq "ndbcluster")
			{
			    #$tables[++$#tables] = $row->{'Tables_in_'.$currDb};
			    $tables[++$#tables] = $row->{'Name'};
			}
		    }
		    else
		    {
				
		    }
#		my $name = $row->{'Name'};
#		$tablesh{$name} =$row->{'Engine'}; 
              }
              
            }
            $sth->finish();
        }
    $tableHashMap{$currDb} = \@tables;
  }
    $Param->{tables} = \%tableHashMap;
    return $Param;
 }
 
 sub getTableStatus($){
    
    my $Param       = shift;
    my %columnHashSource  = %{$Param->{tbColumnStatus}}; 
    my $dbh         = $Param->{dbh};
    my $key         = $Param->{currentDb};
    my $dimTable    = $Param->{currentTable};
    my $sth;
    my $cmd ;
    my @columTypeDim;
    my %columnHash;
    my $Dirty = 2; #0 skip all; 1 backup table ; 2 create new structure
    my $additionalFieldsNumber = 3;

    my $currentFieldsNumber = 0;
    my $newFieldsNumber = (keys(%columnHashSource) + $additionalFieldsNumber);
    
    $cmd = "SELECT count(*) as nrecords FROM information_schema.COLUMNS where table_schema='${key}_audit' and table_name='$dimTable'";
    $sth = $dbh->prepare($cmd);
    $sth->execute();
    
     my $row = undef;
      while ( $row = $sth->fetchrow_hashref() )
      {
        $currentFieldsNumber =  $row->{'nrecords'};
      }
    $sth->finish();
    
    if($newFieldsNumber > 0 && $currentFieldsNumber > 0 && $currentFieldsNumber != $newFieldsNumber)
    {
        return 1;
    }

    $cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='${key}_audit' and table_name='$dimTable'";

    $sth = $dbh->prepare($cmd);
    $sth->execute();

    #my $recordSet = $sth->fetchrow_hashref();
    
    
    #if (defined($recordSet))
    #{
      $row = undef;
      my $newColumns = "";  
      my $oldColumns = "";
      
      while ( $row = $sth->fetchrow_hashref() )
      {
        $Dirty=0;
        
        my $fieldName =  $row->{'COLUMN_NAME'};
        my $dataType  = $row->{'Data_TYPE'};
        my $numericP  = $row->{'NUMERIC_PRECISION'};
        my $charLeng  = $row->{'CHARACTER_MAXIMUM_LENGTH'};
        
        
        if($fieldName ne "operation" && $fieldName ne "counter" && $fieldName ne "recordtype")
        {
        
            if(!defined($columnHashSource{$fieldName}) || $columnHashSource{$fieldName} eq "")
            {
                return 1;
            }
            
            @columTypeDim = @{$columnHashSource{$fieldName}};
            
            if($columTypeDim[0] ne $dataType)
            {
                return 1;    
            }
            
            #CHeck for changed values in the field definition        
            if(defined($columTypeDim[1]) &&  defined($numericP))
            {
                if($columTypeDim[1] != $numericP)
                {
                    return 1;    
                }
            }
    
            #CHeck for changed values in the field definition        
            if((!defined($numericP) && defined($columTypeDim[1])) || (defined($numericP) && !defined($columTypeDim[1])))
            {
                return 1;    
            }
            
            #CHeck for changed values in the field definition
            if(defined($columTypeDim[2]) &&  defined($charLeng))
            {
                if($columTypeDim[2] != $charLeng)
                {
                    return 1;    
                }
            }
            
            if((!defined($charLeng) && defined($columTypeDim[2])) || (defined($charLeng) && !defined($columTypeDim[2])))
            {
                return 1;    
            }
        }
        
#           && ( defined($columTypeDim[2]) && $columTypeDim[2] == $charLeng))
      }
    #}
    $sth->finish();




    return $Dirty;
}
