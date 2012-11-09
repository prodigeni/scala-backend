class QueryExecution < ActiveRecord::Base
  belongs_to :query, :foreign_key => 'query_id'
  serialize :parameters

  # Mapping from ruby-mysql/lib/mysql/constants.rb types to array of Google Visualization column datatype, then QueryTool datatype
  MYSQL_TYPE_MAP = {
      0 => ['number',   'float'     ], # TYPE_DECIMAL 
      1 => ['number',   'integer'   ], # TYPE_TINY 
      2 => ['number',   'integer'   ], # TYPE_SHORT 
      3 => ['number',   'integer'   ], # TYPE_LONG 
      4 => ['number',   'float'     ], # TYPE_FLOAT 
      5 => ['number',   'float'     ], # TYPE_DOUBLE 
      6 => ['string',   'string'    ], # TYPE_NULL 
      7 => ['datetime', 'timestamp' ], # TYPE_TIMESTAMP 
      8 => ['number',   'integer'   ], # TYPE_LONGLONG 
      9 => ['number',   'integer'   ], # TYPE_INT24 
     10 => ['date',     'timestamp' ], # TYPE_DATE
     11 => ['datetime', 'timestamp' ], # TYPE_TIME
     12 => ['datetime', 'timestamp' ], # TYPE_DATETIME
     13 => ['datetime', 'timestamp' ], # TYPE_YEAR
     14 => ['date',     'timestamp' ], # TYPE_NEWDATE
     15 => ['string',   'string'    ], # TYPE_VARCHAR
     16 => ['string',   'string'    ], # TYPE_BIT
    246 => ['number',   'float'     ], # TYPE_NEWDECIMAL 
    247 => ['string',   'string'    ], # TYPE_ENUM 
    248 => ['string',   'string'    ], # TYPE_SET 
    249 => ['string',   'string'    ], # TYPE_TINY_BLOB 
    250 => ['string',   'string'    ], # TYPE_MEDIUM_BLOB 
    251 => ['string',   'string'    ], # TYPE_LONG_BLOB 
    252 => ['string',   'string'    ], # TYPE_BLOB 
    253 => ['string',   'string'    ], # TYPE_VAR_STRING 
    254 => ['string',   'string'    ], # TYPE_STRING 
    255 => ['string',   'string'    ]  # TYPE_GEOMETRY 
  }

  # Mapping from postgres ftypes found here: https://github.com/pszturmaj/ddb/blob/master/postgres.d
  PG_TYPE_MAP = {
      16 => ['string',   'string'    ],
      17 => ['string',   'string'    ],
      18 => ['string',   'string'    ],
      19 => ['string',   'string'    ],
      20 => ['number',   'integer'   ],
      21 => ['number',   'integer'   ],
      23 => ['number',   'integer'   ],
      25 => ['string',   'string'    ],
     700 => ['number',   'float'     ],
     701 => ['number',   'float'     ],
     705 => ['string',   'string'    ],
    1042 => ['string',   'string'    ],
    1043 => ['string',   'string'    ],
    1082 => ['date',     'timestamp' ],
    1083 => ['datetime', 'timestamp' ],
    1114 => ['datetime', 'timestamp' ],
    1184 => ['datetime', 'timestamp' ],
    1186 => ['string',   'string'    ],
    1266 => ['datetime', 'string'    ],
    1700 => ['number',   'float'     ],
    2249 => ['record',   'string'    ],
    2287 => ['record',   'string'    ]
  }

  def execute(options = {:overwrite => false})
    if File.exist?(cache_path) and not options[:overwrite]
      Yajl::Parser.parse(File.read(cache_path))
    else
      start = Time.now
      begin
        data = { 
          'columns' => [], 
          'results' => []
        }   
    
        connection = nil 
        result = nil 
        Timeout.timeout(20.minutes) do
          if (query.source == 'statsdb')
            connection = Statsdb::Base.connection
          else
            connection = StatsdbPg::Base.connection
          end
            logger.info to_sql
          result = connection.execute(to_sql)
        end 
    
        result.each do |row|
          drow = {}
          if (query.source == 'statsdb')
            result.fetch_fields.each_with_index do |f, i|
              drow[f.name] = row[i]
            end 
          else
            result.fields.each_with_index do |f, i|
              drow[f] = row[f]
            end 
          end
          data['results'] << drow
        end 

	if (query.source == 'statsdb')
          data['columns'] = result.fetch_fields.map { |f| f.name }
          data['types']   = result.fetch_fields.map { |f| QueryExecution::MYSQL_TYPE_MAP[f.type][0] || 'string' }
          data['formats'] = result.fetch_fields.map { |f| detect_format(data['results'][0..99].map { |row| row[f.name] }, f.name ) }
        else
          data['columns'] = result.fields
          data['types']   = result.fields.each_with_index.map { |f,i| QueryExecution::PG_TYPE_MAP[result.ftype(i)][0] || 'string' }
          data['formats'] = result.fields.map { |f| detect_format(data['results'][0..99].map { |row| row[f] }, f ) }
        end

        if !query.transform.nil? && query.transform != ''
          t = Transforms.const_get(query.transform.to_sym).new
           data = t.transform(data)
        end

        File.open(cache_path, 'w') { |cache| cache.write(Yajl::Encoder.encode(data)) }
        status = 'Succeeded'
      rescue Yajl::ParseError, StandardError => e
        status = 'Failed'
        Rails.logger.info(e.inspect)
        Rails.logger.info(e.backtrace.join("\n"))
        raise e
      ensure
        finish = Time.now
    
        unless data['results'].blank?
          self.row_count = data["results"].length
          self.file_size = File.size(cache_path)
        end 
    
        self.started_at = start
        self.finished_at = finish
        self.result = status
        self.save
      end

      data
    end
  end

  def detect_format(values, name)
    return 'string' if name =~ /[Ii][Dd]$/
    format_guesses = []
    values.each do |val|
      if val.nil? || val == 'null'
        # do nothing
      elsif val =~ /^[+-]*[0-9.]+$/
        if val =~ /\./
          format_guesses << 'float'
        else
          format_guesses << 'integer'
        end
      elsif val =~ /^[0-9]{4}.[0-9]{2}.[0-9]{2}.[0-9]/
        format_guesses << 'datetime'
      elsif val =~ /^[0-9]{4}.[0-9]{2}.[0-9]{2}/
        format_guesses << 'date'
      else
        format_guesses << 'string'
      end
    end

    if format_guesses.uniq.size == 1
      return format_guesses[0]
    else
      return 'string'
    end
  end

  def to_filename(extension = '.csv')
    query.name.downcase.gsub(/\s+/,'_') + '_' + Time.now.strftime('%Y%m%d_%H%M') + "#{extension}"
  end

  def to_sql(squish = true)
    sql = query.sql.dup
    sql.scan(::Query::GROUPED_PATTERN).to_a.each do |placeholder|
      sql.gsub!(placeholder.first, parameters[placeholder[1]] || placeholder.last)
    end
    sql.squish if squish
    sql
  end

  def parameters
    read_attribute(:parameters) || {}
  end
 
  def cache_path
    if @cache_path
      @cache_path
    else
      path = File.join(query.cache_path, 'executions')
      FileUtils.mkdir_p(path)
      @cache_path = File.join(path, "#{id}.json")
    end
  end

  def clear_cache
    if query
      FileUtils.rm_rf(cache_path)
    end
  end

  def started?
    started_at != nil
  end
  
  def finished?
    finished_at != nil
  end
  
end

