#!perl

# ########################################################################## #
# Title:         Type specification
# Creation date: 2007-03-05
# Author:        Michael Zedeler
# Description:   Class holding type specifications for data streams
# File:          $Source: /data/cvs/lib/DSlib/lib/DS/TypeSpec.pm,v $
# Repository:    kronhjorten
# State:         $State: Exp $
# Documentation: inline
# Recepient:     -
# ########################################################################## #

package DS::TypeSpec;

use base qw{ Clone };

use strict;
use Carp;
use Carp::Assert;
use List::MoreUtils qw{ any all };
use DS::TypeSpec::Field;

our ($VERSION) = $DS::VERSION;
our ($REVISION) = '$Revision: 1.1 $' =~ /(\d+\.\d+)/;


sub new {
    my( $class, $arg1, $arg2 ) = @_;

    my $name;
    my $fields;
    if( $arg1 ) {
        if( ref( $arg1 ) eq '' ) {
            $name = $arg1;
            if( $arg2 ) {
                $fields = $arg2;
            } 
        } else {
           $fields = $arg1;
        }
        if( $fields ) {
            should(ref($fields) , 'ARRAY');
        }
    }

    my $self = bless {
        name    => $name || '',
        fields  => {}
    }, $class;
    
    if( $fields ) {
        $self->add_fields( $fields );
    }
    
    return $self;
}

sub add_fields {
    my( $self, $fields ) = @_;
    
    foreach my $field (@$fields) {
        $self->add_field( $field );
    }
}

sub add_field {
    my( $self, $field ) = @_;
    
    if( ref( $field ) eq '' ) {
        $field = new DS::TypeSpec::Field( $field );
    }
    assert($field->isa('DS::TypeSpec::Field'));
    if( $self->{fields}->{ $field->{name} } ) {
        croak("Can't add field to data stream type spec, since another field with the same name already exists");
    } else {
        $self->{fields}->{ $field->{name} } = $field;
    }
}

sub remove_fields {
    my( $self, $fields ) = @_;
    
    foreach my $field (@$fields) {
        $self->remove_field( $field );
    }
}

sub remove_field {
    my( $self, $field ) = @_;
    
    my $field_name;
    if( not ref($field) eq '' ) {
        should($field->isa, 'DS::TypeSpec::Field');
        $field_name = $field->{name};
    } else {
        $field_name = $field;
    }
    if( not $self->{fields}->{ $field->{name} } ) {
        croak("Can't remove field from data stream type spec - name not recognized. The name is $field_name, but I only have " . join(", ", keys %{$self->{fields}}));
    } else {
        delete $self->{fields}->{ $field->{name} };
    }
}

sub fields {
    my( $self, $fields ) = @_;
    
    my $result = 1;
    if( $fields ) {
        should(ref($fields), 'ARRAY');
        my %remove_fields = ( %{$self->{fields}} );
        foreach my $field ( @$fields ) {
            if( $self->{fields}->{$field} ) {
                $self->add_field( $field );
                delete $remove_fields{ $field };
            }
        }
        $self->{fields} = $fields;
    } else {
        $result = $self->{fields};
    }
    return $result;
}

sub field_names {
    my( $self, $fields ) = @_;
    return keys %{$self->{fields}};
}    

sub keys_locked {
    my( $self, $keys_locked ) = @_;

    my $result = 1;
    if( $keys_locked ) {
       $self->{keys_locked} = $keys_locked ? 1 : 0;
    } else {
        $keys_locked = $self->{keys_locked};
    }
    return $result;
}

sub values_readonly {
    my( $self, $values_readonly ) = @_;

    my $result = 1;
    if( $values_readonly ) {
       $self->{values_readonly} = $values_readonly ? 1 : 0;
    } else {
        $values_readonly = $self->{values_readonly};
    }
    return $result;
}

sub contains {
    my( $self, $other ) = @_;

    my $result;

    if( $other->isa('DS::TypeSpec::Any') ) {
        $result = 1;
    } else {
        # This is equivalent to the subset operator in mathematics
        # For all of the $other fields
        $result = all { 
            my $other = $_;
            # There must be one key with the same name
            any { $_ eq $other } keys %{$self->{fields}}; 
        } keys %{$other->{fields}};
    }
    
    return $result;
}

sub project {
    my( $self, $arg1, $arg2 ) = @_;

    my $name = '';
    my $new_fields;
    if( $arg1 ) {
        if( ref( $arg1 ) eq '' ) {
            $name = $arg1;
            if( $arg2 ) {
                $new_fields = $arg2;
            } 
        } else {
           $new_fields = $arg1;
        }
    }

#    if( ref( $fields ) eq 'ARRAY' ) {
#        my $new_fields = {};
#        foreach my $field ( @$fields ) {
#            $new_fields->{$field} = 1;
#        }
#        $fields = $new_fields;
#    }
    should(ref($new_fields), 'HASH');

    my $new_spec = new DS::TypeSpec( $name );

    foreach my $new_field (keys %$new_fields) {
        if( my $field = $self->{fields}->{ $new_fields->{$new_field} } ) {
             my $new_field_obj = $field->clone();
             $new_field_obj->{name} = $new_field;
             $new_spec->add_field( $new_field_obj );
        } else {
            croak("Can't limit to field $new_field since it is not in the original type");
        }
    }     
    return $new_spec;
}

1;

#TODO Add sorting and unique constraints. Possibly also field order (or maybe not?!?)

__END__
    assert($name !~ /\s/);

    my @pks = ();
    my %pk_lookup = ();
    my @fields = ();
    my %field_lookup = ();

    if(ref($fields) eq 'HASH') {
        foreach my $field (keys %$fields) {
            push @fields, $field;
            $field_lookup{$field} = 1;
            if( $$fields{$field} ) {
                push @pks, $field if $fields->{$field};
                $pk_lookup{$field} = 1 if $fields->{$field};
            }
        }
    } else {
        assert($fields ne "");
    
        foreach my $field_line ( split /\n/, $fields ) {

            next if $field_line =~ /^\s*#/;
            
            my( $field, $is_pk ) = $field_line =~ /\s*(\S+)(?:\s+(\S+))?/;     
    
            push @fields, $field;
            $field_lookup{$field} = 1;
            if(defined($is_pk) and $is_pk == 1 ) {
                push @pks, $field;
                $pk_lookup{$field} = 1;
            }
        }
    }

    $self->set(name => $name);
    $self->set(fields => [@fields]);
    $self->set(field_lookup => {%field_lookup});
    $self->set(pks => [@pks]);
    $self->set(pk_lookup => {%pk_lookup});
    
    return $self;
}


sub set($$$) {
    my($self, $key, $value) = @_;
    
    $self->{$key} = $value;
}


sub get($$) {
    my($self, $key) = @_;

    return $self->{$key};
}

sub project {
    my($self, $name, $fields) = @_;
    
    if( ref( $fields ) eq 'ARRAY' ) {
        my $new_fields = {};
        foreach my $field ( @$fields ) {
            $new_fields->{$field} = 1;
        }
        $fields = $new_fields;
    }
    assert(ref($fields) eq 'HASH');
    
    my $new_fields = {};
    # First check that the projection doesn't require fields we do not already have
    foreach my $field (keys %$fields) {
        # Can't include fields that are not already there
        die "Can't limit to field $field since it is not in the original type" unless exists($self->{field_lookup}->{$field});
    }
    #TODO Ugly bug here: unable to indicate field order in constructor since it takes a hash (keys has no stable order)
    foreach my $field (@{$self->{fields}}) {
        $new_fields->{$field} = defined($self->{pk_lookup}->{$field}) ? 1 : 0;
    }

    return new DS::TypeSpec($name, $new_fields);
}

sub contains {
    my($self, $other) = @_;
    
    assert($other->isa('DS::TypeSpec'));

    my $result = 1;
    foreach my $other_field (@{$other->{fields}}) {
        unless( $self->{field_lookup}->{$other_field} ) {
            $result = 0;
            last;
        }
    }
    
    return $result;
}

1;
