# encoding: utf-8

Bundler.require

require './RequestCache.rb'
require './helpers.rb'

# Interacts with couch db
# uses dumb caching by default
# 
class Couch

  def initialize( options = {} )

    @seed = 78501934531780

    @host      = options[:host]  || ""
    @db        = options[:db]    || ""
    @designDoc = options[:designDoc] || ""

    @cache = unless options[:cache].nil? then options[:cache] else true end

    @login = options[:login] || ""

  end # of initialize

  # Cookie authenticates with AuthSession cookie
  def authenticate( arCookies )

    # authenticate session cookie and get userCtx
    sessionResponse = getRequest( :db => "_session", :cookies => arCookies, :seed => rand(1e25), :login => "")

    if sessionResponse['ok'] && ! sessionResponse['userCtx']['name'].nil?
      return { :valid => true, :name => sessionResponse['userCtx']['name']}
    else
      return { :valid => false }
    end

  end # of authenticate


  def getRequest( arOptions = {} )

    url = getUrl arOptions

    requestOptions = {
      :accept       => :json,
      :content_type => :json
    }

    arOptions[:params] = {} if arOptions[:params].nil?
    arOptions[:params][:key] = "" if arOptions[:params][:key].nil?

    arOptions[:seed] = "" unless arOptions[:seed]

    requestOptions.merge!( arOptions )
    requestKey = "#{url}-#{arOptions[:params][:key]}#{arOptions[:seed]}"
    arOptions.delete(:seed)



    if @cache 
      response = CacheHandler::tryCache(requestKey, lambda {
        return JSON.parse RestClient.get( url, requestOptions ).to_s
      })
    else
      response = JSON.parse RestClient.get( url, requestOptions ).to_s
    end
    return response

  end # of getRequest

  def postRequest( arOptions )

    url  = getUrl arOptions

    arOptions[:content_type] = :json
    arOptions[:accept]       = :json

    data = arOptions[:data]
    arOptions.delete(:data)

    if arOptions[:json].nil?
      json = true
    else
      json = arOptions[:json]
      arOptions.delete(:json)
    end
    

    #data['keys'] = "".to_json unless data['keys']
    arOptions[:seed] = "" unless arOptions[:seed]
    requestKey = "#{url}-#{data['keys']}#{arOptions[:seed]}"
    arOptions.delete(:seed)


    if @cache
      response = CacheHandler::tryCache(requestKey, lambda {
        postResponse = RestClient.post( url, data.to_json, arOptions )
        return JSON.parse postResponse if json
        return postResponse
      })
    else
      postResponse = RestClient.post( url, data.to_json, arOptions )
      if json
        response = JSON.parse postResponse
      else
        response = postResponse
      end
    end

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

    if options[:list]
      url = "http://#{loginString}#{@host}/#{options[:db]}/_design/#{options[:designDoc]}/_list/#{options[:list]}/#{options[:view]}"
    elsif options[:view]
      url = "http://#{loginString}#{@host}/#{options[:db]}/_design/#{options[:designDoc]}/_view/#{options[:view]}"
    elsif options[:document]
      url = "http://#{loginString}#{@host}/#{options[:db]}/#{options[:document]}"
    elsif options[:db]
      url = "http://#{loginString}#{@host}/#{options[:db]}"
    end

    return url

  end # of getUrl

end
