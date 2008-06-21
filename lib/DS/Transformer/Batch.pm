#!perl

# ########################################################################## #
# Title:         Batch-of-rows processor
# Creation date: 2007-03-05
# Author:        Michael Zedeler
#                Henrik Andreasen
# Description:   Process batches of rows in a data stream
#                Data Stream class
#                Data Stream transformer buffer
#                Process batches of rows
# File:          $Source: /data/cvs/lib/DSlib/lib/DS/Transformer/Batch.pm,v $
# Repository:    kronhjorten
# State:         $State: Exp $
# Documentation: inline
# Recepient:     -
# ########################################################################## #

package DS::Transformer::Batch;

use base qw{ DS::Transformer::Buffer };

use strict;
use Carp;

our($VERSION) = $DS::VERSION;
our($REVISION) = '$Revision: 1.1 $' =~ /(\d+\.\d+)/;


# new
#
# Class constructor
#
sub new {
    my( $class, $source, $target ) = @_;

    my $self = $class->SUPER::new( $source, $target );

    $self->{id} = undef;        # Id (delimiter) of current batch or "undef" if no batch exists
    
    return $self;
}


# Process a row, possibly triggering a call to process_batch
#
sub receive_row {
    my ($self, $row) = @_;

    my $id = $self->{id};

    my $row_id;

    # See get delimiter for row if not at end of stream
    if (defined($row)) {
        $row_id = $self->delimiter($row);
    }

    # Process batch if batch id is available and:
    #  - Row id is undef (end of stream)
    #  - Row id differs from batch id
    print STDERR "Check: ($id) ", $self->{current}, "\n";
    if( defined( $id ) and ( not defined( $row_id ) or $row_id ne $id ) ) {
        print STDERR "Fire: ($id) ", $self->{current}, "\n";
        my $result = $self->process_batch($self->{buffer}, $self->{first}, $self->{current}-1);
        $self->flush($self->{current}-1);
        $self->SUPER::receive_row( $row );
    }

    $self->{id} = $row_id;

}


# delimiter
#
sub delimiter {
    croak("You have to override this method: DS::Transformer::Batch->delimiter() to get anything out of this class.\n");
}


# process_batch
#
sub process_batch {
    my ($self, $buffer, $start, $end) = @_;

    croak("You have to override this method: DS::Transformer::Batch->process_batch() to get anything out of this class.\n");

    return 1;
}

1;


