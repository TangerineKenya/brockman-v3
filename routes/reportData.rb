#encoding: utf-8
require 'base64'
require 'date'
require 'json'
require_relative '../helpers/Couch'


class Brockman < Sinatra::Base

  #
  # Start of report
  #

  get '/reportData/:group/:doc.:format?' do | group, doc, format |
    content_type :json
    couch = Couch.new({
      :host      => $settings[:dbHost],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    begin
      docData = couch.getRequest({ :doc => doc, :parseJson => true })
      geoData = { type: 'FeatureCollection', features: docData['data'] }

      return (format == 'geojson' ? geoData : docData).to_json
    rescue => e
      return {error: e}.to_json
    end
  end
end