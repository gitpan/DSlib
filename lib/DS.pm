#!perl

# ########################################################################## #
# Title:         Data Stream base package
# Creation date: 2007-03-05
# Author:        Michael Zedeler
# Description:   Base class for various DS objects. Holds version info as well.
# File:          $Source: /data/cvs/lib/DSlib/lib/DS.pm,v $
# Repository:    kronhjorten
# State:         $State: Exp $
# Documentation: inline
# Recepient:     -
# ########################################################################## #

#TODO Major caveat: it seems that only does lexicographical comparison of version strings when searching for the lastest available module. This is plain wrong and will result in annoying errors. Always specify which version to use when using only.pm.

package DS;

use strict;
use warnings;

use Carp::Assert;
use Exporter 'import';

our( $VERSION, $REVISION, $STATE );

BEGIN {
    # This is THE version of the this package as a whole
    $__PACKAGE__::VERSION = '2.12';
    ($__PACKAGE__::STATE) = '$State: Exp $' =~ /:\s+(.+\S)\s+\$$/;

    # Sets local package version info
    $VERSION = $__PACKAGE__::VERSION;
    ($REVISION) = '$Revision: 1.2 $' =~ /(\d+\.\d+)/;
    $STATE = $__PACKAGE__::STATE;

    warn("WARNING: this code has been marked as being experimental.") if $STATE eq 'Exp';
}

our @EXPORT_OK = qw( chain_start chain_end );

sub chain_start {
    my( $target ) = @_;
    
    assert( $target->isa('DS::Target'), 'Must provide a DS::Target object' );
    my $result = $target;
    $result = $result->source while( $result->source );    

    return $result;
}

sub chain_end {
    my( $source ) = @_;
    
    assert( $source->isa('DS::Source'), 'Must provide a DS::Source object' );
    my $result = $source;
    $result = $result->target while( $result->target );    

    return $result;
}

1;

__END__
=pod

=head1 NAME

DS - Data Stream module

=head1 DESCRIPTION

This package provides a framework for writing data processing components
that work on typed streams. A typed stream in DS is a stream of hash 
references where every hashreference obeys certain constraints that is
contained in a type specification.

=head1 MISSING DOCUMENTATION

This module is in perfect working order and has been used in production
environments for a year at this time of writing, but there is no 
documentation yet.

=head1 APIS SUBJECT TO CHANGE

I have decidede to pursue a more general way of writing transformers
which will be available in version 3 of this package. I am certain that
some APIs will be changed in a way that is not backwards compatible.

=head1 AUTHOR

Written by Michael Zedeler.
