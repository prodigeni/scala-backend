module Transforms
  class WeekToColumn

    def transform(data)
      key_columns = data['columns'] - ['Week', 'Value']
      data['columns'] = key_columns.dup

      dates = data['results'].map { |row| row['Week'] }.uniq.sort
      data['columns'].concat( dates.map { |d| DateTime.strptime(d, "%Y-%m-%d").strftime("%b %d %Y") } )

      ordered_keys = data['results'].map { |row| key_columns.map { |col| row[col] }.join(',') }.uniq

      key_values = {}

      data['results'].each do |row|
        key_values[ key_columns.map { |col| row[col] }.join(',') ] ||= Hash[key_columns.map { |col| [ col, row[col] ] }]
        key_values[ key_columns.map { |col| row[col] }.join(',') ][ DateTime.strptime(row['Week'], "%Y-%m-%d").strftime("%b %d %Y") ] = row['Value']
      end

      data['results'] = ordered_keys.map { |k| key_values[k] }
      data['types']   = key_columns.each_with_index.map { |c,i| data['types'][i] }.concat( dates.map { |d| 'number' } )
      data['formats'] = key_columns.each_with_index.map { |c,i| data['formats'][i] }.concat( dates.map { |d| 'integer' } )

      data
    end

  end
end
