#!/usr/bin/perl

# ########################################################################## #
# Title:         Build time tests of batch transformer
# Creation date: 2007-04-13
# Author:        Michael Zedeler
# Description:   Runs tests of DS::Transformer::Batch
# File:          $Source: /data/cvs/lib/DSlib/t/40_batch_transformer.t,v $
# Repository:    kronhjorten
# State:         $State: Exp $
# Documentation: inline
# Recepient:     -
# ########################################################################## #

use strict;
use warnings;
use Test::More tests => 10;

BEGIN {
        $|  = 1;
        $^W = 1;
}

use_ok( 'DS::Transformer::Batch' );

use DS::Importer::Sub;
use DS::TypeSpec;
use DS::TypeSpec::Field;
use DS::Target::Sink;

#
# The following sets up a generator that fires off
# $max_rows number of rows with field count starting at
# $count and value starting at $value
#
my $in_type = DS::TypeSpec->new('mytable', [
    new DS::TypeSpec::Field( 'count' ),
    new DS::TypeSpec::Field( 'value' )
]);

my $out_type = DS::TypeSpec->new('mytable', [
    new DS::TypeSpec::Field( 'batch' )
]);

my $count;
my $value;
my $max_rows;

my $importer = new DS::Importer::Sub( 
    sub {
        if( $count <= $max_rows ) {
            return {count => $count++, value => $value++};
        } else {
            return undef;
        }
    },
    $in_type
);

ok( $importer );

my $sink = new DS::Target::Sink;

ok( $sink );

my $batch_processor;

ok( $batch_processor = new BatchTest( 
        $importer,
        $sink
    )
) or diag("Unable to instantiate batch processor class");

my $row;

$value = 10;

diag("Check degenerate case: no input rows at all");
$max_rows = -1;
$count = 0;
$main::batch_number = 0;
$batch_processor = new BatchTest( 
        $importer, 
        $sink
);
# We expect that the batch processor passes undef to us
# If it did pass anything, that would mean that process_batch had been called
# (See implementation of BatchTest below.)
$importer->execute();

diag("Check case where number of rows is divisible by batch size");
$max_rows = 8;
$count = 0;
$main::batch_number = 0;
$batch_processor = new BatchTest( 
        $importer, 
        $sink
);
$importer->execute(1);
is( $sink->{batch}, undef);
is( $main::batch_number, 0 );
$importer->execute(1);
is( $sink->{batch}, undef);
is( $main::batch_number, 0 );
$importer->execute(1);
is( $sink->{batch}, undef);
is( $main::batch_number, 0 );
$importer->execute(1);
is( $sink->{batch}, 0);
is( $main::batch_number, 1 );
diag("Here\n");
exit;

diag("Off by one test 1:");
diag("    Check case where number of rows - 1 is divisible by batch size");
$max_rows = 10;
$count = 0;
$main::batch_number = 0;
$batch_processor = new BatchTest( 
        $importer, 
        $sink
);
ok( $row = $batch_processor->fetch() ); 
is( $row->{batch}, undef);
is( $main::batch_number, 0 );
ok( $row = $batch_processor->fetch() ); 
is( $row->{batch}, undef);
is( $main::batch_number, 0 );
ok( $row = $batch_processor->fetch() ); 
is( $row->{batch}, undef);
is( $main::batch_number, 0 );
exit;

diag("Off by one test 2:");
diag("    Check case where number of rows + 1 is divisible by batch size");
$max_rows = 8;
$count = 0;
$main::batch_number = 0;
$batch_processor = new BatchTest( 
        $importer, 
        $sink
);
ok( $row = $batch_processor->fetch() ); 
is( $row->{batch}, 0);
ok( $row = $batch_processor->fetch() ); 
is( $row->{batch}, 1);
ok( $row = $batch_processor->fetch() ); 
is( $row->{batch}, 2);
is( $batch_processor->fetch(), undef ); 

package BatchTest;
use base 'DS::Transformer::Batch';

sub delimiter {
    my( $self, $row ) = @_;
    my $result = int($row->{count}/3);
    return $result;
}

sub process {
    my( $self, $row ) = @_;
    $self->SUPER::process( $row );
}

sub process_batch {
    my( $self, $buffer, $start, $end ) = @_;
    return {batch => $main::batch_number++};
}

1;
