class CreateQueries < ActiveRecord::Migration
  def change
    create_table :queries do |t|
      t.string  :category
      t.string  :name
      t.text    :description
      t.text    :source
      t.text    :sql
      t.text    :parameters
      t.string  :transform
      t.timestamps
    end
  end
end

