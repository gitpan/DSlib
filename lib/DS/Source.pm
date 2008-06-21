#!perl

# ########################################################################## #
# Title:         Data stream generator
# Creation date: 2007-03-05
# Author:        Michael Zedeler
# Description:   Generates data stream data
#                Data Stream class
# File:          $Source: /data/cvs/lib/DSlib/lib/DS/Source.pm,v $
# Repository:    kronhjorten
# State:         $State: Exp $
# Documentation: inline
# Recepient:     -
# ########################################################################## #

package DS::Source;

use strict;
use Carp;
use Carp::Assert;

our ($VERSION) = $DS::VERSION;
our ($REVISION) = '$Revision: 1.1 $' =~ /(\d+\.\d+)/;
our ($STATE) = '$State: Exp $' =~ /:\s+(.+\S)\s+\$$/;


sub new {
    my( $class, $out_type, $target ) = @_;

    my $self = {
        row => {}
    };
    bless $self, $class;

    if( defined( $out_type ) ) {
        $self->out_type( $out_type );
    }

    if( defined( $target ) ) {
        $self->attach_target( $target );
    }

    return $self;
}

sub attach_target {
    my( $self, $target ) = @_;

    assert( $target->isa('DS::Target') );
    # First break link with old target, if any
    if( $self->{target} ) {
        $self->{target}->{source} = undef;
    }
    if( $target->source( $self ) ) {
        $self->target( $target );
    }
}

# This is a primarily private method
# Important caveat: this method is just a field accessor method.
# Maintaining consistent links with target is handled by attach_target
sub target {
    my( $self, $target ) = @_;

    my $result;
    if( $target ) {
        assert($target->isa('DS::Target'));
        $self->{target} = $target;
        $result = 1;
    } else {
        $result = $self->{target};
    }
    return $result;        
}

# Send row to target
sub pass_row {
    my( $self, $row ) = @_;
    confess("Can't pass rows since no target has been set") unless $self->target;
    $self->target()->receive_row( $row );
}

sub out_type {
    my( $self, $type ) = @_;

    my $result;
    if( $type ) {
        assert($type->isa('DS::TypeSpec'));
        $self->{out_type} = $type;
    } else {
        $result = $self->{out_type};
    }
    return $result;        
}

1;
