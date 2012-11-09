class CreateQueryExecutions < ActiveRecord::Migration
  def change
    create_table  :query_executions do |t|
      t.integer   :query_id
      t.text      :sql
      t.text      :parameters
      t.timestamp :started_at
      t.timestamp :finished_at
      t.integer   :row_count
      t.integer   :file_size
      t.string    :result
      t.text      :notes
      t.timestamps
    end
  end
end
