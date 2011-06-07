package Loop::Sustainable;

use strict;
use warnings;

use Carp;
use Exporter qw(import);
use Class::Load qw(load_class);
use String::RewritePrefix;
use Time::HiRes qw(tv_interval gettimeofday);

our $VERSION = '0.01';
our @EXPORT  = qw(loop_sustainable);

sub loop_sustainable (&&;$) {
    my ( $cb, $terminator, $args ) = @_;

    $args ||= +{};
    %$args = (
        wait_interval => 0.1,
        check_strategy_interval => 10,
        strategy => +{
            class => 'ByLoad',
            args  => +{ load => 0.5 },
        },
        %$args,
    );

    my $strategy_cb;

    if ( ref $args->{strategy} eq 'HASH' ) {
        my ( $strategy_class ) = String::RewritePrefix->rewrite(+{ 
            '' => 'Loop::Sustainable::Strategy::', '+' => ''
        }, $args->{strategy}{class});

        load_class( $strategy_class );

        my $strategy = $strategy_class->new( 
            check_strategy_interval => $args->{check_strategy_interval}, 
            %{$args->{strategy}{args}} 
        );

        $strategy_cb = sub {
            my ( $execute_count, $time_sum, $rv ) = @_;
            $strategy->wait_correction( $execute_count, $time_sum, $rv );
        };
    }
    elsif ( ref $args->{strategy} eq 'CODE' ) {
        $strategy_cb = $args->{strategy};
    }
    else {
        croak 'Not supported strategy type. The strategy field must be hash reference or code reference.';
    }

    my $i               = 1;
    my $time_sum        = 0;
    my $time_total      = 0;
    my $wait_interval   = $args->{wait_interval};
    my $additional_wait = 0;

    for (;;) {
        my $t0      = [ Time::HiRes::gettimeofday ];
        my @ret = $cb->( $i, $wait_interval );
        my $elapsed = Time::HiRes::tv_interval( $t0, [ Time::HiRes::gettimeofday ] );

        $time_sum   += $elapsed;
        $time_total += $elapsed;

        if ( $terminator->( $i, $time_sum, \@ret ) ) {
            last;
        }

        Time::HiRes::sleep($wait_interval);

        if ( $i % $args->{check_strategy_interval} == 0 ) {
            $additional_wait = $strategy_cb->( $i, $time_sum, \@ret );
            $wait_interval   = $args->{wait_interval} + $additional_wait;
            $time_sum        = 0;
        }

        $i++;
    }

    my %result = ( 
        executed => $i, 
        total_time => $time_total 
    );

    return wantarray ? %result : \%result;

}

1;
__END__

=head1 NAME

Loop::Sustainable - Loop callback sustainably.

=head1 SYNOPSIS

  use DBI;
  use Loop::Sustainable;

  my $dbh = DBI->connect( ... );
  my $result = loop_sustainable(
      sub {
          my ( $execute_count, $time_sum ) = @_;
          my $rv = $dbh->do( 'DELETE FROM large_table ORDER BY id ASC LIMIT 100' );
          $dbh->commit or die( $dbh->errstr );
          return $rv;
      },
      sub {
          my ( $execute_count, $time_sum, $rv ) = @_;
          $rv->[0] < 100 ? 1 : 0;
      },
      +{
          wait_interval => 0.1,
          check_strategy_interval => 10,
          strategy => +{
            class => 'ByLoad',
            args  => +{ load => 0.5 },
          },
      }
  );

  printf("executed: %d; total time: %.02f sec\n", $result->{executed}, $result->{total_time});

=head1 DESCRIPTION

Loop::Sustainable runs loop sustainably. Loop::Sustainable only exports loop_sustainable() function.

=head1 METHODS

=head2 loop_sustainable( \&cb, \&terminator, \%args )

This function runs callback function several times until terminator is returned true value.
And this function sleeps in order to specified strategy. So the loop supplied callback can run sustainably and continually.

=over

=item \&cb( $execute_count, $time_sum )

$execute_count is loop count. $time_sum is total times until calling strategy's wait_correction() method.

=item \&terminator( $execute_count, $time_sum, $rv )

$execute_count and $time_sum are same means in \&cb.
$rv is array reference as \&cb return values.

=item \%args

Available keys are below.

=over

=item wait_interval

The base waiting time after running each loop.

=item check_strategy_interval

The count of next check time by strategy.

=back

=back

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
