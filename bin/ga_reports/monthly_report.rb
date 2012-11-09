#!/usr/bin/ruby

require 'wikia_report'

@subject = "Monthly Google Analytics report"
@end_date = Time.now.beginning_of_month
@start_date = @end_date - 1.year
@results = {}
@dates = []
@wikia_users = [ 'ga-monthly-analytics-report@wikia-inc.com' ]

def map_cities
  wikis = {}
  puts "Fetching Cities"
  City.eager(:categories, :tags).all do |wiki|
    city_id = wiki.city_id
    wikis[city_id] = {}
    wikis[city_id][:pageviews] = {}
    wikis[city_id][:dbname] = wiki.city_dbname
    wikis[city_id][:lang] = wiki.city_lang
    wikis[city_id][:url] = wiki.city_url
    wikis[city_id][:cat] = ''
    wikis[city_id][:tags] = []    

    if wiki.categories && wiki.categories.first
      wikis[city_id][:cat] = wiki.categories.first.cat_name #Only one on the backend
    end

    if wiki.tags
      wiki.tags.each do |tag|
        wikis[city_id][:tags] << tag.name
      end
    end
  end
  return wikis
end


results = map_cities

begin
  puts "Processing: #{@start_date.month}/#{@start_date.day}/#{@start_date.year}"
  pvs = Pageviews.filter(:date => @start_date)
  month = @start_date.month
  year = @start_date.year
  pvs.each do |pv|
    if pv.city_id && pv.pageviews && results[pv.city_id]
      if results[pv.city_id][:pageviews].has_key?("#{month}/#{year}")
        results[pv.city_id][:pageviews]["#{month}/#{year}"] += pv.pageviews
      else
        results[pv.city_id][:pageviews]["#{month}/#{year}"] = pv.pageviews
      end
    end
  end
  @dates << "#{month}/#{year}" unless @dates.include?("#{month}/#{year}")
  @start_date = @start_date + 1.day
end while @start_date < @end_date


Dir.mktmpdir("monthly-stats", "/tmp") do |tmpdir|
  FasterCSV.open("#{tmpdir}/wikis.csv", "w") do |csv|
    puts "Writing out results"
    csv << ["City_ID", "DBName", "Category", "URL", "Lang", "Tags",  @dates].flatten
    results.sort.each do |city_id,city|
      pageviews = []
      @dates.each do |d|
        pv = city[:pageviews][d]
        if pv
          pageviews << pv
        else
          pageviews << 0
        end
      end
      csv << [ city_id, city[:dbname], city[:cat], city[:url], city[:lang], city[:tags].join(','), pageviews ].flatten
    end  
  end
  Dir.chdir(tmpdir)
  system("zip wikis.csv.zip wikis.csv")
  send_msg("#{tmpdir}/wikis.csv.zip", @wikia_users, @subject)
end

