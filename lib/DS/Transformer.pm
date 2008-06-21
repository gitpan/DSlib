#!perl

# ########################################################################## #
# Title:         Data stream transformer
# Creation date: 2007-03-05
# Author:        Michael Zedeler
# Description:   Transforms data stream
#                Data Stream class
# File:          $Source: /data/cvs/lib/DSlib/lib/DS/Transformer.pm,v $
# Repository:    kronhjorten
# State:         $State: Exp $
# Documentation: inline
# Recepient:     -
# ########################################################################## #

package DS::Transformer;

use strict;
use Carp::Assert;

our ($VERSION) = $DS::VERSION;
our ($REVISION) = '$Revision: 1.1 $' =~ /(\d+\.\d+)/;

require DS::TypeSpec;
require DS::Target;
require DS::Source;

our( @ISA ) = qw{ DS::Target DS::Source };


sub new {
    my( $class, $in_type, $out_type, $source, $target ) = @_;

    bless my $self = {}, $class;

    if( $in_type ) {
        $self->in_type( $in_type );
    }
    if( $out_type ) {
        $self->out_type( $out_type );
    }
    if( $source ) {
        $self->attach_source( $source );
    }
    if( $target ) {
        $self->attach_target( $target );
    }

    return $self;
}

# Override this method if you want to change how the transformer passes
# rows onto its target when this method is called. If you just want to
# transform the row without changing how data is passed on, override
# process() in stead.
# This method MUST NOT return anything. If errors occur, croak or die with exceptions
sub receive_row {
    my( $self, $row ) = @_;

    $self->pass_row( $self->process( $row ) );

    return;
}

# Process row (possibly transforming it) before passing it to
# the next transformer.
# Just no operation (this method is here to be overridden)
sub process {
    return $_[1];
}

1;

