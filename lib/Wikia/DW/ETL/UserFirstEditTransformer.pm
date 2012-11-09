package Wikia::DW::ETL::UserFirstEditTransformer;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Transformer );

use Wikia::DW::ETL::Database;

sub transform {
    my ($self, $data) = @_;

    $self->{db} ||= Wikia::DW::ETL::Database->new( source => 'statsdb' );

    push @{ $data->{columns} }, 'first_edit_date'    if @{$data->{columns}}[-1] ne 'first_edit_ts';
    push @{ $data->{columns} }, 'first_edit_wiki_id' if @{$data->{columns}}[-1] ne 'first_edit_ts';
    push @{ $data->{columns} }, 'first_edit_domain'  if @{$data->{columns}}[-1] ne 'first_edit_ts';
    push @{ $data->{columns} }, 'first_edit_title'   if @{$data->{columns}}[-1] ne 'first_edit_ts';
    push @{ $data->{columns} }, 'first_edit_ts'      if @{$data->{columns}}[-1] ne 'first_edit_ts';

    foreach my $r (@{ $data->{rows} }) {
        my $result = $self->{db}->arrayref(
           "SELECT MIN(r.time_id) AS first_edit_date
              FROM rollup_wiki_namespace_user_events r
             WHERE r.period_id = 1
               AND r.time_id >= '2012-07-01'
               AND r.namespace_id = 0 
               AND r.user_id = $r->{user_id}
               AND r.edits > 0"
        );
        if (scalar @$result > 0) {
            $r->{first_edit_date} = $result->[0]->{first_edit_date};

            if ($r->{first_edit_date}) {
                $result = $self->{db}->arrayref(
                   "SELECT w.wiki_id    AS first_edit_wiki_id,
                           w.domain     AS first_edit_domain,
                           a.title      AS first_edit_title,
                           sub.event_ts AS first_edit_ts
                      FROM (SELECT e.user_id,
                                   e.wiki_id,
                                   e.namespace_id,
                                   e.article_id,
                                   e.event_ts
                              FROM fact_event_events e
                             WHERE e.event_ts >= '$r->{first_edit_date}'
                               AND e.event_ts < DATE_ADD('$r->{first_edit_date}', INTERVAL 1 DAY)
                               AND e.event_type = 'edit'
                               AND e.user_id = $r->{user_id}
                               AND e.namespace_id = 0
                               AND e.article_id < 1000000000
                             ORDER BY e.event_ts
                             LIMIT 1) sub
                      LEFT JOIN dimension_wiki_articles a
                        ON a.wiki_id = sub.wiki_id
                       AND a.namespace_id = sub.namespace_id
                       AND a.article_id = sub.article_id
                      LEFT JOIN statsdb_mart.dimension_wikis w
                        ON w.wiki_id = sub.wiki_id"
                );
                $r->{first_edit_wiki_id} = $result->[0]->{first_edit_wiki_id};
                $r->{first_edit_domain}  = $result->[0]->{first_edit_domain};
                $r->{first_edit_title}   = $result->[0]->{first_edit_title};
                $r->{first_edit_ts}      = $result->[0]->{first_edit_ts};
            }
        }
    }
    return;
}

1;
