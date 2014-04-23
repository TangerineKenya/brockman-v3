# encoding: utf-8

###
#   _  _
#  / `/_`| /  /|/|  _  _  /_ ._  _ 
# /_,._/ |/  /   | /_|/_ / /// //_'
#
# This program makes CSV files using Tangerine's CouchDB views.
###

require 'rubygems'
require 'sinatra/base'
require 'sinatra/cross_origin'
require "sinatra/cookies"
require 'securerandom'
require 'premailer'
require 'mail'
require 'rest-client'
require 'json'
require 'logger'
require './config.rb'
require './helpers.rb'
require './Couch.rb'
require './Csv.rb'
require './RequestCache.rb'

#
#
#


$logger = Logger.new "CsvMachine.log"
class CsvMachine < Sinatra::Base
  helpers Sinatra::Cookies

  set :allow_origin  => :any,
      :allow_methods => [:get, :post, :options],
      :allow_credentials => true,
      :max_age => "1728000",
      :protection => { :except => :json_csrf },
      :port => 3141

  get "/" do
    output "csv", false
  end

  get '/cache/clear' do
    RequestCache.destroy
  end

  get '/csvs/:group' do
    group = params[:group].gsub(/\//, '')
    Dir["*.csv"]
  end

  get '/csv/:group/:filename' do | group, fileName |

    throw "Not found" if /\.\.\//.match( fileName )

    send_file fileName, {
      :filename => fileName
    }

  end

  get "/report/:group/:workflowIds/:year/:month/:county.:format?" do | group, workflowIds, year, month, county, format |

    requestId = SecureRandom.base64

    $logger.info "(#{requestId}) Monthly - #{group}"

    couch = Couch.new({
      :host      => $settings[:host],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    geography = couch.getRequest({ :document => "geography-quotas" })
    quotasByZones  = {}
    quotasByCounty = {}
    quotaNational = 0

    geography['counties'].map { | countyName, county |
      quotasByCounty[countyName.downcase] = county['quota']
      county['zones'].map { | zone, quota |
        quotasByZones[zone.downcase] = quota
        quotaNational += quota.to_i
      }
    }

    byZone = {}

    # get trips from month specified
    monthKey        = "\"year#{year}month#{month}\""
    tripsFromMonth  = couch.getRequest({ :view => "tutorTrips", :params => { :key => monthKey } } )

    tripIds = tripsFromMonth['rows'].map{ |e| e['value'] }

    # if workflows specified, filter trips to those workflows
    if workflowIds != "all"
      workflowKey       = workflowIds.split(",").map{ |s| "workflow-#{s}" }
      tripsFromWorkflow = couch.postRequest({ :view => "tutorTrips", :data => { "keys" => workflowKey } } )['rows'].map{ |e| e['value'] }
      tripIds           = tripIds & tripsFromWorkflow
    end

    # get summaries from trips
    tripKeys      = tripIds.uniq
    tripsResponse = couch.postRequest({ :view => "spirtRotut?group=true", :data => { "keys" => tripKeys } } )

    tripRows = tripsResponse['rows']

    #
    # filter rows
    #

    tripRows = tripRows.select { | row |
      longEnough = ( row['value']['maxTime'].to_i - row['value']['minTime'].to_i ) / 1000 / 60 >= 20
      longEnough
    }

    # used for keys for this request
    monthGroup = "#{group}#{year}#{month}"

    result = {}

    result['visits'] = CacheHandler::tryCache "visits-#{monthGroup}-#{tripKeys.join}", lambda {
      byZone = {}
      byCounty = {}
      national = 0

      for sum in tripRows
        next if sum['value']['zone'].nil?
        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase

        byZone[zoneName] = 0 unless byZone[zoneName]
        byZone[zoneName] += 1

        byCounty[countyName] = 0 unless byCounty[countyName]
        byCounty[countyName] += 1

        national += 1 

      end
      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }
    }

    result['zonesByCounty'] = CacheHandler::tryCache "zones-by-county-#{monthGroup}-#{tripKeys.join}", lambda {
      counties = {}
      for sum in tripRows

        next if sum['value']['zone'].nil?

        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase

        counties[countyName] = [] unless counties[countyName]
        counties[countyName].push(zoneName) unless counties[countyName].include?(zoneName)
      end
      return counties
    }


    result['fluency'] = CacheHandler::tryCache "fluency-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone = {}
      byCounty = {}
      national = {}
      subjects = []

      for sum in tripRows

        next if sum['value']['zone'].nil? or sum['value']['itemsPerMinute'].nil?

        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase
        itemsPerMinute = sum['value']['itemsPerMinute']
        benchmarked    = sum['value']['benchmarked']

        subject = sum['value']['subject']

        subjects.push subject if not subjects.include? subject

        p subject


        total = 0
        itemsPerMinute.each { | ipm | total += ipm }

        byZone[zoneName]          = {}        unless byZone[zoneName]
        byZone[zoneName][subject] = {}        unless byZone[zoneName][subject]
        byZone[zoneName][subject]['sum']  = 0 unless byZone[zoneName][subject]['sum']
        byZone[zoneName][subject]['size'] = 0 unless byZone[zoneName][subject]['size']

        byZone[zoneName][subject]['sum']  += total
        byZone[zoneName][subject]['size'] += benchmarked

        byCounty[countyName]                  = {} unless byCounty[countyName]
        byCounty[countyName][subject]         = {} unless byCounty[countyName][subject]
        byCounty[countyName][subject]['sum']  = 0  unless byCounty[countyName][subject]['sum']
        byCounty[countyName][subject]['size'] = 0  unless byCounty[countyName][subject]['size']

        byCounty[countyName][subject]['sum']  += total
        byCounty[countyName][subject]['size'] += benchmarked

        national                  = {} unless national
        national[subject]         = {} unless national[subject]
        national[subject]['sum']  = 0  unless national[subject]['sum']
        national[subject]['size'] = 0  unless national[subject]['size']

        national[subject]['sum']  += total
        national[subject]['size'] += benchmarked



      end

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national,
        "subjects" => subjects
      }
    } # result['fluency']


    result['metBenchmark'] = CacheHandler::tryCache "met-benchmark-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone   = {}
      byCounty = {}
      national = 0

      for sum in tripRows

        next if sum['value']['zone'].nil?

        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase

        met = sum['value']['metBenchmark']

        byZone[zoneName] = 0 unless byZone[zoneName]
        byZone[zoneName] += met

        byCounty[countyName] = 0 unless byCounty[countyName]
        byCounty[countyName] += met

        national += met

      end

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }

    } # result['metBenchmark']


    #
    # Output
    #

    return result.to_json if format == "json"

    if county.downcase == "all"
      zones = result['visits']['byZone'].keys
    else
      zones = result['zonesByCounty'][county.downcase]
    end

    zoneTableHtml = "
      <table>
        <thead>
          <tr>
            <th>Zone</th>
            <th># of classroom visits</th>
            <th>Targeted number of classroom visits</th>
            #{result['fluency']['subjects'].map{ | subject |
              "<th>Average items per minute - #{subject}</th>"
            }.join}
            
            <th>Students that met benchmark</th>
            <th>% of those assessed</th>

          </tr>
        </thead>
        <tbody>
          #{zones.map{ |zone|

            zone = zone.downcase

            next if result['fluency']['byZone'][zone].nil?

            visits = result['visits']['byZone'][zone]

            met = result['metBenchmark']['byZone'][zone]

            quota = quotasByZones[zone]

            sampleTotal = 0

          "
            <tr>
              <td>#{zone}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].map{ | subject |
                sample = result['fluency']['byZone'][zone][subject]
                
                if sample && sample['size'] != 0 && sample['sum'] != 0
                  sampleTotal += sample['size']
                  average = ( sample['sum'] / sample['size'] ).round
                else
                  average = ''
                end
                "<td>#{average}</td>"
              }.join}

              <td>#{met}</td>
              <td>#{(met / sampleTotal) * 100}</td>
            </tr>
          "}.join }
        </tbody>
      </table>

    "

    countyTableHtml = "
      <table>
        <thead>
          <tr>
            <th>County</th>
            <th># of classroom visits</th>
            <th>Targeted number of classroom visits</th>
            #{result['fluency']['subjects'].map{ | subject |
              "<th>Average items per minute - #{subject}</th>"
            }.join}
            <th>Students that met benchmark</th>
            <th>% of those assessed</th>

          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | county, visits |

            county = county.downcase

            met = result['metBenchmark']['byCounty'][county]

            quota = quotasByCounty[county]

            sampleTotal = 0

          "
            <tr>
              <td>#{county}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].map{ | subject |
                sample = result['fluency']['byCounty'][county][subject]
                if sample && sample['size'] != 0 && sample['sum'] != 0
                  sampleTotal += sample['size']
                  average = ( sample['sum'] / sample['size'] ).round
                else
                  average = ''
                end
                "<td>#{average}</td>"
              }.join}
              <td>#{met}</td>
              <td>#{(met / sampleTotal) * 100}</td>
            </tr>
          "}.join }
        </tbody>
      </table>
    "

    nationalTableHtml = "
      <table>
        <thead>
          <tr>
            <th># of classroom visits</th>
            <th>Targeted number of classroom visits</th>
            <th>Average items per minute</th>
            <th>Students that met benchmark</th>
            <th>% of those assessed</th>

          </tr>
        </thead>
        <tbody>
          #{
            visits = result['visits']['national']
            size = result['fluency']['national']['size']
            sum = result['fluency']['national']['sum']
            met = result['metBenchmark']['national']

            size = 1 if size == 0 or size.nil?
            sum  = 1 if sum  == 0 or sum.nil?

            if met
              percentageMet = (met / size) * 100
            else
              percentageMet = 0
            end
          "
            <tr>
              <td>#{visits}</td>
              <td>#{quotaNational}</td>
              <td>#{(sum/size).round}</td>
              <td>#{met}</td>
              <td>#{percentageMet}</td>
            </tr>
          "}
        </tbody>
      </table>
    "

    return "
    <html>
    <head>

    <style>body{font-family:Helvetica;}</style>

    <link rel='stylesheet' type='text/css' href='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/css/jquery.dataTables.css'>

    <script src='http://code.jquery.com/jquery-1.11.0.min.js'></script>
    <script src='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/jquery.dataTables.min.js'></script>
    <script>
      $(document).ready(function() {
        $('table').dataTable(#{if format == 'flat' then "{\"iDisplayLength\":-1}" end});

        $('select').on('change',function() {
          year   = $('#year-select').val().toLowerCase()
          month  = $('#month-select').val().toLowerCase()
          county = $('#county-select').val().toLowerCase()
          document.location = 'http://#{$settings[:host]}/_csv/report/#{group}/#{workflowIds}/'+year+'/'+month+'/'+county+'.html'
        }
)

      } );
    </script>

    </head>

    <body>
      <h1><img style='vertical-align:middle;' src=\"http://databases.tangerinecentral.org/tangerine/_design/ojai/images/corner_logo.png\" title=\"Go to main screen.\"> Kenya National Tablet Programme</h1>

      <label for='year-select'>Year</label>
      <select id='year-select'>
        <option #{"selected" if year == "2013"}>2013</option>
        <option #{"selected" if year == "2014"}>2014</option>
      </select>

      <label for='month-select'>Month</label>
      <select id='month-select'>
        <option value='1'  #{"selected" if month == "1"}>Jan</option>
        <option value='2'  #{"selected" if month == "2"}>Feb</option>
        <option value='3'  #{"selected" if month == "3"}>Mar</option>
        <option value='4'  #{"selected" if month == "4"}>Apr</option>
        <option value='5'  #{"selected" if month == "5"}>May</option>
        <option value='6'  #{"selected" if month == "6"}>Jun</option>
        <option value='7'  #{"selected" if month == "7"}>Jul</option>
        <option value='8'  #{"selected" if month == "8"}>Aug</option>
        <option value='9'  #{"selected" if month == "9"}>Sep</option>
        <option value='10' #{"selected" if month == "10"}>Oct</option>
        <option value='11' #{"selected" if month == "11"}>Nov</option>
        <option value='12' #{"selected" if month == "12"}>Dec</option>
      </select>

      <h2>National</h2>
      #{nationalTableHtml}

      <br><br>

      <h2>Counties</h2>
      #{countyTableHtml}

      <br><br>

      <h2>Zones</h2>
      <label for='county-select'>Select county</label>
      <select id='county-select'>
        <option value='all'>All</option>
        #{
          result['zonesByCounty'].map { |county, zones|
            "<option #{"selected" if county == params[:county]}>#{county}</option>"
          }.join("")
        }
      </select>

      #{zoneTableHtml}



      </body>
    </html>
    "

    return html


  end

  get '/email/:email/:group/:workflowIds/:year/:month/:county' do | email, group, workflowIds, year, month, county |


    requestId = SecureRandom.base64

    $logger.info "(#{requestId}) email - #{group}"

    couch = Couch.new({
      :host      => $settings[:host],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    geography = couch.getRequest({ :document => "geography-quotas" })
    quotasByZones  = {}
    quotasByCounty = {}
    quotaNational = 0

    geography['counties'].map { | countyName, county |
      quotasByCounty[countyName.downcase] = county['quota']
      county['zones'].map { | zone, quota |
        quotasByZones[zone.downcase] = quota
        quotaNational += quota.to_i
      }
    }

    byZone = {}

    # get trips from month specified
    monthKey        = "\"year#{year}month#{month}\""
    tripsFromMonth  = couch.getRequest({ :view => "tutorTrips", :params => { :key => monthKey } } )

    tripIds = tripsFromMonth['rows'].map{ |e| e['value'] }

    # if workflows specified, filter trips to those workflows
    if workflowIds != "all"
      workflowKey       = workflowIds.split(",").map{ |s| "workflow-#{s}" }
      tripsFromWorkflow = couch.postRequest({ :view => "tutorTrips", :data => { "keys" => workflowKey } } )['rows'].map{ |e| e['value'] }
      tripIds           = tripIds & tripsFromWorkflow
    end

    # get summaries from trips
    tripKeys      = tripIds.uniq
    tripsResponse = couch.postRequest({ :view => "spirtRotut?group=true", :data => { "keys" => tripKeys } } )

    tripRows = tripsResponse['rows']

    #
    # filter rows
    #

    tripRows = tripRows.select { | row |
      longEnough = ( row['value']['maxTime'].to_i - row['value']['minTime'].to_i ) / 1000 / 60 >= 20
      longEnough
    }

    # used for keys for this request
    monthGroup = "#{group}#{year}#{month}"

    result = {}

    result['visits'] = CacheHandler::tryCache "visits-#{monthGroup}-#{tripKeys.join}", lambda {
      byZone = {}
      byCounty = {}
      national = 0

      for sum in tripRows
        next if sum['value']['zone'].nil?
        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase

        byZone[zoneName] = 0 unless byZone[zoneName]
        byZone[zoneName] += 1

        byCounty[countyName] = 0 unless byCounty[countyName]
        byCounty[countyName] += 1

        national += 1 

      end
      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }
    }


    result['fluency'] = CacheHandler::tryCache "fluency-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone = {}
      byCounty = {}
      national = {}
      subjects = []

      for sum in tripRows

        next if sum['value']['zone'].nil? or sum['value']['itemsPerMinute'].nil?

        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase
        itemsPerMinute = sum['value']['itemsPerMinute']
        benchmarked    = sum['value']['benchmarked']

        subject = sum['value']['subject']

        subjects.push subject if not subjects.include? subject

        total = 0
        itemsPerMinute.each { | ipm | total += ipm }

        byZone[zoneName]          = {}        unless byZone[zoneName]
        byZone[zoneName][subject] = {}        unless byZone[zoneName][subject]
        byZone[zoneName][subject]['sum']  = 0 unless byZone[zoneName][subject]['sum']
        byZone[zoneName][subject]['size'] = 0 unless byZone[zoneName][subject]['size']

        byZone[zoneName][subject]['sum']  += total
        byZone[zoneName][subject]['size'] += benchmarked

        byCounty[countyName]                  = {} unless byCounty[countyName]
        byCounty[countyName][subject]         = {} unless byCounty[countyName][subject]
        byCounty[countyName][subject]['sum']  = 0  unless byCounty[countyName][subject]['sum']
        byCounty[countyName][subject]['size'] = 0  unless byCounty[countyName][subject]['size']

        byCounty[countyName][subject]['sum']  += total
        byCounty[countyName][subject]['size'] += benchmarked

        national                  = {} unless national
        national[subject]         = {} unless national[subject]
        national[subject]['sum']  = 0  unless national[subject]['sum']
        national[subject]['size'] = 0  unless national[subject]['size']

        national[subject]['sum']  += total
        national[subject]['size'] += benchmarked



      end

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national,
        "subjects" => subjects
      }
    } # result['fluency']

    p result['fluency']['subjects'].to_json

    result['metBenchmark'] = CacheHandler::tryCache "met-benchmark-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone   = {}
      byCounty = {}
      national = 0

      for sum in tripRows

        next if sum['value']['zone'].nil?

        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase

        met = sum['value']['metBenchmark']

        byZone[zoneName] = 0 unless byZone[zoneName]
        byZone[zoneName] += met

        byCounty[countyName] = 0 unless byCounty[countyName]
        byCounty[countyName] += met

        national += met

      end

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }

    } # result['metBenchmark']



    result['zonesByCounty'] = CacheHandler::tryCache "zones-by-county-#{monthGroup}-#{tripKeys.join}", lambda {
      counties = {}
      for sum in tripRows

        next if sum['value']['zone'].nil?

        zoneName   = sum['value']['zone'].downcase
        countyName = sum['value']['county'].downcase

        counties[countyName] = [] unless counties[countyName]
        counties[countyName].push(zoneName) unless counties[countyName].include?(zoneName)
      end
      return counties
    }

    zones = result['zonesByCounty'][county.downcase].sort_by{|word| word.downcase}

    row = 0

    subjectLegend = { "word" => "Kiswahili", "english_word" => "English", "operation" => "Math" }

    zoneTableHtml = "
      <table class='dataTable'>
        <thead>
          <tr>
            <th class='sorting'>Zone</th>
            <th class='sorting'># of classroom visits</th>
            <th class='sorting'>Targeted number of classroom visits</th>
            #{result['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
              "<th class='sorting'>Average items per minute - #{subjectLegend[subject]}</th>"
            }.join}
            
            <th class='sorting'>Students that met benchmark</th>
            <th class='sorting'>% of those assessed</th>

          </tr>
        </thead>
        <tbody>
          #{zones.map{ |zone|

            row += 1

            zone = zone.downcase

            next if result['fluency']['byZone'][zone].nil?

            visits = result['visits']['byZone'][zone]

            met = result['metBenchmark']['byZone'][zone]

            quota = quotasByZones[zone]

            sampleTotal = 0

          "
            <tr class='#{if row % 2 == 0 then "even" else "odd" end}'>
              <td>#{zone.capitalize}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
                sample = result['fluency']['byZone'][zone][subject]
                
                if sample && sample['size'] != 0 && sample['sum'] != 0
                  sampleTotal += sample['size']
                  average = ( sample['sum'] / sample['size'] ).round
                else
                  average = ''
                end
                "<td>#{average}</td>"
              }.join}

              <td>#{met}</td>
              <td>#{(met / sampleTotal) * 100}</td>
            </tr>
          "}.join }
        </tbody>
      </table>

    "


    html =  "
    <html>
      <head>
        <style>
          body{font-family:Helvetica;}
          table.dataTable{margin:0 auto;clear:both;width:100%}table.dataTable thead th{padding:3px 18px 3px 10px;border-bottom:1px solid #000;font-weight:700;cursor:pointer;*cursor:hand}table.dataTable tfoot th{padding:3px 18px 3px 10px;border-top:1px solid #000;font-weight:700}table.dataTable td{padding:3px 10px}table.dataTable td.center,table.dataTable td.dataTables_empty{text-align:center}table.dataTable tr.odd{background-color:#E2E4FF}table.dataTable tr.even{background-color:#fff}table.dataTable tr.odd td.sorting_1{background-color:#D3D6FF}table.dataTable tr.odd td.sorting_2{background-color:#DADCFF}table.dataTable tr.odd td.sorting_3{background-color:#E0E2FF}table.dataTable tr.even td.sorting_1{background-color:#EAEBFF}table.dataTable tr.even td.sorting_2{background-color:#F2F3FF}table.dataTable tr.even td.sorting_3{background-color:#F9F9FF}.dataTables_wrapper{position:relative;clear:both;*zoom:1}.dataTables_length{float:left}.dataTables_filter{float:right;text-align:right}.dataTables_info{clear:both;float:left}.dataTables_paginate{float:right;text-align:right}.paginate_disabled_next,.paginate_disabled_previous,.paginate_enabled_next,.paginate_enabled_previous{height:19px;float:left;cursor:pointer;*cursor:hand;color:#111!important}.paginate_disabled_next:hover,.paginate_disabled_previous:hover,.paginate_enabled_next:hover,.paginate_enabled_previous:hover{text-decoration:none!important}.paginate_disabled_next:active,.paginate_disabled_previous:active,.paginate_enabled_next:active,.paginate_enabled_previous:active{outline:0}.paginate_disabled_next,.paginate_disabled_previous{color:#666!important}.paginate_disabled_previous,.paginate_enabled_previous{padding-left:23px}.paginate_disabled_next,.paginate_enabled_next{padding-right:23px;margin-left:10px}.paginate_enabled_previous{background:url(../images/back_enabled.png) no-repeat top left}.paginate_enabled_previous:hover{background:url(../images/back_enabled_hover.png) no-repeat top left}.paginate_disabled_previous{background:url(../images/back_disabled.png) no-repeat top left}.paginate_enabled_next{background:url(../images/forward_enabled.png) no-repeat top right}.paginate_enabled_next:hover{background:url(../images/forward_enabled_hover.png) no-repeat top right}.paginate_disabled_next{background:url(../images/forward_disabled.png) no-repeat top right}.paging_full_numbers{height:22px;line-height:22px}.paging_full_numbers a:active{outline:0}.paging_full_numbers a:hover{text-decoration:none}.paging_full_numbers a.paginate_active,.paging_full_numbers a.paginate_button{border:1px solid #aaa;-webkit-border-radius:5px;-moz-border-radius:5px;border-radius:5px;padding:2px 5px;margin:0 3px;cursor:pointer;*cursor:hand;color:#333!important}.paging_full_numbers a.paginate_button{background-color:#ddd}.paging_full_numbers a.paginate_button:hover{background-color:#ccc;text-decoration:none!important}.paging_full_numbers a.paginate_active{background-color:#99B3FF}.dataTables_processing{position:absolute;top:50%;left:50%;width:250px;height:30px;margin-left:-125px;margin-top:-15px;padding:14px 0 2px;border:1px solid #ddd;text-align:center;color:#999;font-size:14px;background-color:#fff}.sorting{background:url(../images/sort_both.png) no-repeat center right}.sorting_asc{background:url(../images/sort_asc.png) no-repeat center right}.sorting_desc{background:url(../images/sort_desc.png) no-repeat center right}.sorting_asc_disabled{background:url(../images/sort_asc_disabled.png) no-repeat center right}.sorting_desc_disabled{background:url(../images/sort_desc_disabled.png) no-repeat center right}table.dataTable thead td:active,table.dataTable thead th:active{outline:0}.dataTables_scroll{clear:both}.dataTables_scrollBody{*margin-top:-1px;-webkit-overflow-scrolling:touch}
        </style>
      </head>

      <body>
        <h1><img style='vertical-align:middle;' src=\"http://databases.tangerinecentral.org/tangerine/_design/ojai/images/corner_logo.png\" title=\"Go to main screen.\"> Kenya National Tablet Programme</h1>

        <h2>
          #{county.capitalize} County Report
          #{year} #{["","Jan","Feb","Mar","Apr","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
        </h2>
        #{zoneTableHtml}

        </body>
      </html>
      "

    premailer = Premailer.new(html, 
      :with_html_string => true, 
      :warn_level => Premailer::Warnings::SAFE
    )
    mailHtml = premailer.to_inline_css
    if email
      
      mail = Mail.deliver do
        to      email
        from    'Tangerine <no-reply@tangerinecentral.org>'
        subject 'Report for your county'

        html_part do
          content_type 'text/html; charset=UTF-8'
          body mailHtml
        end
      end

    end
    mailHtml


  end


  get '/workflow/:group/:workflowId' do | groupPath, workflowId |

    requestId = SecureRandom.base64

    $logger.info "(#{requestId}) CSV request - #{groupPath} #{workflowId}"

    couch = Couch.new({
      :host      => $settings[:host],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => groupPath,
      :local     => $settings[:local]
    })


    #
    # Authentication
    #

    authenticate = couch.authenticate(cookies)

    unless authenticate[:valid] == true
      $logger.info "(#{requestId}) Authentication failed"
      status 401
      output "who?", $loginUrl
      return authenticate[:valid]
    end

    $logger.info "(#{requestId}) User #{authenticate[:name]} authenticated"

    #
    # get workflow
    #

    workflow = couch.getRequest({ :document => workflowId })
    workflowName = workflow['name']
    $logger.info "(#{requestId}) Beginning #{workflowName}"

    #
    # Get csv rows from view
    #

    # get all results associated with workflow
    resultRows = couch.postRequest({
      :view => "resultsByWorkflowId",
      :data => { "keys" => [workflowId] }
    })['rows']

    $logger.info "(#{requestId}) Received #{resultRows.length} result ids"

    # group results together by trip
    resultsByTripId = {}

    # save all results for bulk fetch from csvRows
    allResultIds = []

    for row in resultRows

      tripId   = row['value']
      resultId = row['id']

      resultsByTripId[tripId] = [] if resultsByTripId[tripId].nil?
      resultsByTripId[tripId].push resultId
      allResultIds.push resultId

    end

    # fetch all results
    allResults = couch.postRequest({
      :view => "csvRows",
      :data => { "keys" => allResultIds }
    })

    $logger.info "(#{requestId}) Received #{allResults['rows'].length} results"

    # for easy lookup
    allResultsById = Hash[allResults['rows'].map { |row| [row['id'], row['value'] ] }]

    $logger.info "(#{requestId}) Processing start"

    csv = Csv.new({
      :name => workflowName,
      :path => groupPath
    })

    file = csv.doWorkflow({
      :allResultsById  => allResultsById,
      :resultsByTripId => resultsByTripId
    })

    $logger.info "(#{requestId}) Done, returning value"

    send_file file[:uri], { :filename => "#{groupPath[6..-1]}-#{file[:name]}" }

  end


  #
  # Make CSVs for regular assessments
  #

  get '/assessment/:group/:assessmentId' do | group, assessmentId |

    requestId = SecureRandom.base64

    # authenticate user
    unless authenticate[:valid] == true
      $logger.info "(#{requestId}) Authentication failed"
      status 401
      output "who?", $loginUrl
      return authenticate[:valid]
    end

    groupPath = calcGroupName group

    assessmentName = JSON.parse(RestClient.get("http://#{$login}@#{$host}/#{groupPath}/#{assessmentId}"))['name']

    # Get csv rows for klass
    csvData = {
      :keys => [assessmentId]
    }

    csvRowResponse = JSON.parse(RestClient.post("http://#{$login}@#{$host}/#{groupPath}/_design/ojai/_view/csvRows",csvData.to_json, :content_type => :json,:accept => :json ))

    columnNames = []
    machineNames = []
    csvRows = []

    for result in csvRowResponse['rows']

      row = []

      for cell in result['value']

        key         = cell['key']
        value       = cell['value']
        machineName = cell['machineName']
        unless machineNames.include?(machineName)
          machineNames.push machineName
          columnNames.push key
        end

        index = machineNames.index(machineName)

        row[index] = value

      end

      csvRows.push row

    end

    csvRows.unshift(columnNames)
    csvData = ""
    for row in csvRows
      csvData += row.map { |title|
        "\"#{title.to_s.gsub(/"/,'”')}\""
      }.join(",") + "\n"
    end

    unless params[:download] == "false"
      response.headers["Content-Disposition"] = "attachment;filename=#{assessmentName} #{timestamp()}.csv"
      response.headers["Content-Type"] = "application/octet-stream"
    end


    return csvData



  end



  get '/class/:group/:id' do

    # authenticate user
    authenticate = authenticate()
    return authenticate[:response] unless authenticate[:response] === true

    group   = params[:group]
    klassId = params[:id]

    groupPath = calcGroupName group

    # get students hashed by id
    studentResponse = JSON.parse(RestClient.post("http://#{$login}@#{$host}/#{groupPath}/_design/ojai/_view/byCollection",{"keys"=>["student"]}.to_json, :content_type => :json,:accept => :json ))
    studentsById = Hash[ studentResponse['rows'].map { | row | [ row['id'], row['value'] ] } ]

    # get the curriculum id for this class
    klassResponse = JSON.parse(RestClient.get("http://#{$login}@#{$host}/#{groupPath}/#{params[:id]}",{ :content_type => :json,:accept => :json }))
    curriculumId = klassResponse['curriculumId']

    # get subtests from curriculum, hash by id
    subtestResponse = JSON.parse(RestClient.post("http://#{$login}@#{$host}/#{groupPath}/_design/ojai/_view/subtestsByAssessmentId",{"keys"=>["#{curriculumId}"]}.to_json, :content_type => :json,:accept => :json ))
    subtestsById = Hash[ subtestResponse['rows'].map { | row | [ row['value']['_id'], row['value'] ] } ]

    # Get csv rows for klass
    csvData = {
      :keys => [klassId]
    }
    csvRows = JSON.parse(RestClient.post("http://#{$login}@#{$host}/#{groupPath}/_design/ojai/_view/csvRows",csvData.to_json, :content_type => :json,:accept => :json ))

    studentRows = {}
    columnHeaders = ["student_id", "student_name"]


    for row in csvRows["rows"]

      for obj in row['value']
        studentId = obj['value']
        break if obj['machineName'] == "universal-studentId"
      end

      studentRows[studentId] = [] if studentRows[studentId].nil?

      studentRows[studentId].push row

    end


    columnNames = []
    machineNames = []
    csvRows = []


    studentRows.each { | studentId, results |

      row = []

      results.each_with_index { | result, resultIndex |

        for cell in result['value']

          key         = cell['key']
          value       = cell['value']
          machineName = cell['machineName']+resultIndex.to_s
          unless machineNames.include?(machineName)
            machineNames.push machineName
            columnNames.push key
          end

          index = machineNames.index(machineName)

          row[index] = value

        end

      }

      csvRows.push row

    }


    csvRows.unshift(columnNames)
    csvData = ""
    for row in csvRows
      csvData += row.map { |title|
        "\"#{title.to_s.gsub(/"/,'”')}\""
      }.join(",") + "\n"
    end

    response.headers["Content-Disposition"] = "attachment;filename=class #{timestamp()}.csv"
    response.headers["Content-Type"]        = "application/octet-stream"

    return csvData

  end

  run! if app_file == $0

end # of class



