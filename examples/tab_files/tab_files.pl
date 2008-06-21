# Everything should be in an explicit package
package main;

# Safeguards against common errors
use strict;
use warnings;

# Easy way to point to modules relative to script location
use FindBin qw { $Bin };
use lib "$Bin/../../lib";

# Load various modules from DS
use DS::Importer::TabFile;
use DS::Target::Sink;
use DS::Transformer::Sub;
use DS::Transformer::TabFileWriter;
use DS::TypeSpec;

# Create an importer that reads from tab separated file
my $importer = new DS::Importer::TabFile( "$Bin/languages.txt" );

# Examine what fields the data stream importer provides
my $fields = $importer->out_type->fields;
print 'Fields in file: ', join(', ', keys %$fields ), ".\n";

# Set up a transformer that adds integer values found in stream
# and adds a field with this sum
my $sum_transformer = new DS::Transformer::Sub(
    # Anonymous subroutine
    sub {
        my( $self, $row ) = @_;

        # $row may be set to undef which indicated end of stream
        # in that case, skip computation
        if( $row ) {
            my $sum = 0;
            foreach my $value (values %$row) {
                $sum += $1 if( $value =~ /(\d+)/ );
            }
            # Carefully create a new row
            $row = {%$row, sum => $sum};
        }
        
        return $row;
    },
    # This transformer will accept any incoming type
    $importer->out_type,
    # It adds a field named 'sum'
    new DS::TypeSpec( [ keys %$fields, 'sum' ] ),
    # And we want to use the importer as a source
    $importer
);

# Set up a small transformer that prints to screen
my $print_transformer = new DS::Transformer::Sub(
    sub {
        my( $self, $row ) = @_;
        print join(", ", %$row), "\n" if( $row );
        return $row;
    },
    # No changes to data
    $sum_transformer->out_type,
    $sum_transformer->out_type,
    # Source
    $sum_transformer
);

# Set up tabular file writer
my $exporter = new DS::Transformer::TabFileWriter(
    # File name
    "$Bin/languages-sum.txt",
    # Field order (must be specified at time of writing)
    [ sort (keys %$fields, 'sum') ],
    # Source
    $print_transformer,
    # Dummy target
    new DS::Target::Sink()
);

# Now the processing tree has been set up.

# This will read the input file and process everything.
$importer->execute();
