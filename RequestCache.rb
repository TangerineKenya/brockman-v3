# encoding: utf-8

require './helpers.rb'
require 'rest-client'
require 'data_mapper'


DataMapper::Logger.new("#{File.join(Dir.pwd, 'brockman.log')}", :debug)

DataMapper.setup(:default, "sqlite://#{File.join(Dir.pwd,'brockman.db')}")
DataMapper::Model.raise_on_save_failure = true
DataMapper::Property::Text.length(2000000) 

class RequestCache

  include DataMapper::Resource

  property :request_key, Integer, :key  => true
  property :response,    Object
  property :created_at,  DateTime

end

#DataMapper.auto_migrate!
DataMapper.auto_upgrade!

DataMapper.finalize


module CacheHandler

  def self.tryCache(key="", process)

    requestKey = XXhash.xxh32(key, $settings[:seed])
    if cache = RequestCache.get(requestKey)
      return cache.attributes[:response]
    else
      result = process.call()
      RequestCache.create({
        :request_key => requestKey,
        :response    => result,
        :created_at  => Time.now
      })
      return result
    end

  end

end