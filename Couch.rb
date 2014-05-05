# encoding: utf-8

require 'rest-client'
require 'xxhash'
require './RequestCache.rb'
require './helpers.rb'
class Couch

  def initialize( options = {} )

    @seed = 78501934531780

    @host      = options[:host]  || ""
    @db        = options[:db]    || ""
    @designDoc = options[:designDoc] || ""

    @login = options[:login] || ""

  end # of initialize

  def authenticate( arCookies )

    # authenticate session cookie and get userCtx
    sessionResponse = getRequest( :db => "_session", :cookies => arCookies )

    # if they're not logged in
    if sessionResponse['ok']
      userCtx = sessionResponse["userCtx"]
      name    = userCtx['name']
      unless name
        return { :valid => false }
      end
    end

    return { :valid => true, :name => name }

  end # of authenticate


  def getRequest( arOptions = {} )

    url = getUrl arOptions

    requestOptions = {
      :accept       => :json,
      :content_type => :json
    }

    arOptions[:params] = {} if arOptions[:params].nil?
    arOptions[:params][:key] = "" if arOptions[:params][:key].nil?

    requestOptions.merge!( arOptions )
    requestKey = "#{url}-#{arOptions[:params][:key]}"

    response = CacheHandler::tryCache(requestKey, lambda {
      return JSON.parse RestClient.get( url, requestOptions ).to_s
    })

    return response

  end # of getRequest

  def postRequest( arOptions )

    url  = getUrl arOptions

    arOptions[:content_type] = :json
    arOptions[:accept]       = :json

    data = arOptions[:data]
    arOptions.delete(:data)

    data['keys'] = "" unless data['keys']
    requestKey = "#{url}-#{data['keys']}"

    response = CacheHandler::tryCache(requestKey, lambda {
      postResponse = RestClient.post( url, data.to_json, arOptions )
      return JSON.parse postResponse
    })



    return response

  end # of postRequest

  private

  def getUrl(arOptions)

    login = arOptions[:login] || @login

    if login
      loginString = login + "@"
    else
      loginString = ''
    end

    options = arOptions

    # use defaults where no specifics
    options[:db]        = @db        unless options[:db]
    options[:designDoc] = @designDoc unless options[:designDoc]

    if options[:view]
      url = "http://#{loginString}#{@host}/#{options[:db]}/_design/#{options[:designDoc]}/_view/#{options[:view]}"
    elsif options[:document]
      url = "http://#{loginString}#{@host}/#{options[:db]}/#{options[:document]}"
    elsif options[:db]
      url = "http://#{loginString}#{@host}/#{options[:db]}"
    end

    return url

  end # of getUrl

end