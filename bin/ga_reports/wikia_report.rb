require 'rubygems'
require 'active_support/all'
require 'fastercsv'
require 'sequel'
require 'mail'
require 'mime/types'
require 'tmpdir'

GA_DB = Sequel.mysql('google_analytics', :user => 'google_analytics',
                  :host => 'statsdb-s3', :password => 'LaddArd5')
WIKI_DB = Sequel.mysql('wikicities', :user => 'wikia_mw',
                  :host => '10.8.32.26', :password => 'ahShu8eb')

class Pageviews < Sequel::Model
  set_dataset GA_DB[:pageviews]
end

class TagMap < Sequel::Model
  set_primary_key [ :city_id, :tag_id ]
  set_dataset WIKI_DB[:city_tag_map].select(:city_id, :tag_id)
  many_to_one :tag, :key => :tag_id
  many_to_one :city, :key => :city_id
end

class CatMap < Sequel::Model
  set_primary_key [ :city_id, :cat_id ]
  set_dataset WIKI_DB[:city_cat_mapping].select(:city_id, :cat_id)
  many_to_one :category, :key => :cat_id
  many_to_one :city, :key => :city_id
end

class City < Sequel::Model
  set_primary_key :city_id
  set_dataset WIKI_DB[:city_list].select(:city_id, :city_dbname, :city_lang, :city_url, :city_created)
  one_to_many :domains
  many_to_many :categories, :join_table => :city_cat_mapping,
  :left_key => :city_id, :left_primary_key => :city_id,
  :right_key => :cat_id, :right_primary_key => :cat_id
  many_to_many :tags, :join_table => :city_tag_map,
  :left_key => :city_id, :left_primary_key => :city_id,
  :right_key => :tag_id, :right_primary_key => :id
end

class Domain < Sequel::Model
  set_primary_key :city_id
  set_dataset WIKI_DB[:city_domains].select(:city_id, :city_domain)
  many_to_one :city
end

class Category < Sequel::Model
  set_primary_key :cat_id
  set_dataset WIKI_DB[:city_cats].select(:cat_id, :cat_name)
  many_to_many :cities, :join_table => :city_cat_mapping,
  :left_key => :cat_id, :left_primary_key => :cat_id,
  :right_key => :city_id, :right_primary_key => :city_id
end

class Tag < Sequel::Model
  set_primary_key :id
  set_dataset WIKI_DB[:city_tag].select(:id, :name)
  many_to_many :cities, :join_table => :city_tag_map,
  :left_key => :tag_id, :left_primary_key => :id,
  :right_key => :city_id, :right_primary_key => :city_id
end

def send_msg (file, recipients, subject)
  puts "Sending mail"
  mail = Mail.new do
    from     'Wikia Analytics <analytics@wikia-inc.com>'
    to       recipients
    subject  subject
    add_file :filename => 'wikis.cvs.zip', :content => File.read(file)
  end
  mail.delivery_method :sendmail, :arguments => '-i'
  mail.deliver
end

