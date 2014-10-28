require './config.rb'
require './Couch.rb'


couch = Couch.new({
  :cache     => false,
  :host      => $settings[:host],
  :login     => $settings[:login],
  :designDoc => $settings[:designDoc],
  :db        => 'group-fake_tutor',
  :local     => $settings[:local]
})


requestOptions = {
  :view => "resultsByWorkflowId",
  :data => { "keys" => ["c835fc38-de99-d064-59d3-e772ccefcf7d"] },
  :params => { "limit" => 1 }
}

iterator = CouchIterator.new({
  :couch          => couch,
  :requestOptions => requestOptions,
  :chunkSize      => 621,
  :method         => 'post'
})

puts iterator.refreshTotalRows()
puts iterator.hasChunk()
puts iterator.getChunk()
puts iterator.hasChunk()
