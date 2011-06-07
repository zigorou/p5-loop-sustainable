package Loop::Sustainable::Strategy::MySQL::BalancedReplication;

use strict;
use warnings;
use parent qw(Loop::Sustainable::Strategy);

use Carp;
use Class::Accessor::Lite (
    new => 0,
    rw  => [qw/dbh capable_behind_seconds on_error_scale_factor on_error_croak/],
);
use List::Util qw(max);
use Try::Tiny;

our $VERSION = '0.01';

sub new {
    my $class = shift;
    my $self = $class->SUPER::new(@_);
    unless ( exists $self->{capable_behind_seconds} ) {
        $self->{capable_behind_seconds} = 5;
    }
    unless ( exists $self->{on_error_scale_factor} ) {
        $self->{on_error_scale_factor} = 5;
    }
    unless ( exists $self->{on_error_croak} ) {
        $self->{on_error_croak} = 0;
    }
    $self;
}

sub wait_correction {
    my ( $self, $i, $elapsed, $rv ) = @_;

    my $second_behind_master; ;

    try {
        my $dbh = $self->{dbh};
        my $status = $dbh->selectrow_hashref('SHOW SLAVE STATUS') or croak($dbh->errstr);
        if ( defined $status->{Seconds_Behind_Master} ) {
            $second_behind_master = $status->{Seconds_Behind_Master};
        }
    }
    catch {
        my $e = $_;
        if ( $self->{on_error_croak} ) {
            croak $e;
        }
        else {
            carp $e;
        }
    };

    unless ( defined $second_behind_master ) {
        $second_behind_master = max($self->{capable_behind_seconds}, 5) * $self->{on_error_scale_factor};
    }

    return max( ( $second_behind_master - $self->{capable_behind_seconds} ) / $self->check_strategy_interval, 0 );
}

1;

__END__

=head1 NAME

Loop::Sustainable::Strategy::MySQL::BalancedReplication - write short description for Loop::Sustainable::Strategy::MySQL::BalancedReplication

=head1 SYNOPSIS

  use Loop::Sustainable::Strategy::MySQL::BalancedReplication;

=head1 DESCRIPTION
q
=head1 METHODS

=head1 METHODS

=head2 new( %args )

=over

=item dbh

L<DBI::db> object

=item capable_behind_seconds

=item on_error_scale_factor

=back

=head2 wait_correction( $query, $time_sum, $executed_count )

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@dena.jp<gt>

=head1 LICENSE

This module is licensed under the same terms as Perl itself.

=head1 SEE ALSO

=cut

# Local Variables:
# mode: perl
# perl-indent-level: 4
# indent-tabs-mode: nil
# coding: utf-8-unix
# End:
#
# vim: expandtab shiftwidth=4:
