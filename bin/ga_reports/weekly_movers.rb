#!/usr/bin/ruby

require 'wikia_report'

@end_date = Time.now.monday
@start_date = @end_date - 2.week
@results = {}
@dates = []
@wikia_users = [ 'ga-weekly-small-wiki-movers-report@wikia-inc.com' ]

@subject = "Weekly Small Wiki Movers report"

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
    wikis[city_id][:created] = wiki.city_created
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
  day = @start_date.day
  month = @start_date.month
  year = @start_date.year
  puts "Processing: #{month}/#{day}/#{year}"
  7.times do |i|
    rep_date = @start_date + i.day
    puts " #{rep_date}"
    pvs = Pageviews.filter(:date => rep_date)
    pvs.each do |pv|
      if pv.city_id && pv.pageviews && results[pv.city_id]
        if results[pv.city_id][:pageviews].has_key?("#{month}/#{day}/#{year}")
          results[pv.city_id][:pageviews]["#{month}/#{day}/#{year}"] += pv.pageviews
        else
          results[pv.city_id][:pageviews]["#{month}/#{day}/#{year}"] = pv.pageviews
        end
      end
    end
  end
  @dates << "#{month}/#{day}/#{year}" unless @dates.include?("#{month}/#{day}/#{year}")
  @start_date = @start_date + 1.week
end while @start_date < @end_date

Dir.mktmpdir("weekly-stats", "/tmp") do |tmpdir|
  FasterCSV.open("#{tmpdir}/wikis.csv", "w") do |csv|
    puts "Writing out results"
    csv << ["City_ID", "DBName", "Category", "URL", "Tags", @dates, "Change"].flatten
    results.sort.each do |city_id,city|
      pageviews = []
      largest = 0
      @dates.each do |d|
        pv = city[:pageviews][d]
        if pv
          pageviews << pv
          largest = pv if pv > largest
        else
          pageviews << 0
        end
      end
      if largest < 10000 && city[:lang] == "en" && city[:created]
        if city[:created] > (Time.now - 2.week) && city[:created] < (Time.now - 1.week)
          curr = city[:pageviews][@dates[1]]
          past = city[:pageviews][@dates[0]]
          change = ((curr.to_f - past.to_f) / past.to_f) * 100.0
          change = 0.0 if change.nan? || change.infinite?
          percent = sprintf("%.1f",change)
          csv << [ city_id, city[:dbname], city[:cat], city[:url], city[:tags].join(','), pageviews, percent ].flatten
        end
      end
    end  
  end
  Dir.chdir(tmpdir)
  system("zip wikis.csv.zip wikis.csv")
  send_msg("#{tmpdir}/wikis.csv.zip", @wikia_users, @subject)
end

