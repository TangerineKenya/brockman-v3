#encoding: utf-8

require 'uri'
require 'base64'
require 'date'
require_relative '../helpers/Couch'
require_relative '../utilities/countyTranslate'
require_relative '../utilities/zoneTranslate'
require_relative '../utilities/percentage'
require_relative '../utilities/pushUniq'


class Brockman < Sinatra::Base

  #
  # Start of report
  #

  get '/reimbursement/:group/:workflowIds/:year/:month/:county/:zone.:format?' do | group, workflowIds, year, month, county, zone, format |
  
    format = "html" unless format == "json"

    safeCounty = county
    safeZone = zone
    
    begin
     county = Base64.urlsafe_decode64 params[:county].downcase
    rescue
     county = params[:county].downcase
    end
    
    begin
     zone = Base64.urlsafe_decode64 params[:zone].downcase
    rescue
     zone = params[:zone].downcase
    end

    requestId = SecureRandom.base64

    TRIP_KEY_CHUNK_SIZE = 500

    couch = Couch.new({
      :host      => $settings[:dbHost],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    subjectLegend = { "english_word" => "English", "word" => "Kiswahili", "operation" => "Math" }

    #
    # get Group settings - for time zone calculation
    #
    groupSettings = couch.getRequest({ :doc => 'settings', :parseJson => true })
    groupTimeZone = groupSettings['timeZone'] 

    #
    # Get quota information
    # 
    begin
      result = couch.getRequest({ :doc => "report-aggregate-year#{year}month#{month}", :parseJson => true })
    rescue => e
      # the doc doesn't already exist
      puts e
      return invalidReport()
    end

    currentCounty         = nil
    currentCountyName     = county

    currentZone           = nil
    currentZoneName       = zone
   
    #ensure that the county in the URL is valid - if not, select the first
    if result['visits']['byCounty'][currentCountyName].nil?
      result['visits']['byCounty'].find { |countyName, county|
        currentCounty     = county
        currentCountyName = countyName.downcase
        true
      }
    else 
      currentCounty = result['visits']['byCounty'][currentCountyName]
    end

    #ensure that the zone in the URL is valid - if not, select the first
    if result['visits']['byCounty'][currentCountyName]['zones'][currentZoneName].nil?
      result['visits']['byCounty'][currentCountyName]['zones'].find { |zoneName, zone| 
        currentZone       = zone
        currentZoneName   = zoneName.downcase
        true
      }
    else
      currentZone = result['visits']['byCounty'][currentCountyName]['zones'][currentZoneName]  
    end

    # Get trips from current zone
    zoneTripIds = result['visits']['byCounty'][currentCountyName]['zones'][currentZoneName]['trips'].map{ |e| e }

    # get the real data
    tripsResponse = couch.postRequest({
      :view => "spirtRotut",
      :params => { "group" => true },
      :data => { "keys" => zoneTripIds },
      :parseJson => true,
      :cache => true
    } )
    zoneTrips = tripsResponse['rows']

    #retrieve a county list for the select and sort it
    countyList = []
    result['visits']['byCounty'].map { |countyName, county| countyList.push countyName }
    countyList.sort!

    #retrieve a zone list for the select and sort it
    zoneList = []
    result['visits']['byCounty'][currentCountyName]['zones'].map { |zoneName, zone| zoneList.push zoneName }
    zoneList.sort!



    nationalTableHtml = "
      <table id='nationalTable'>
        <thead>
          <tr>
            <th>County</th>
            <th>Number of classroom visits / valid</th>
            <th>Targeted number of classroom visits / valid</th>
            <th>Total Reimbursement</th>
          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | countyName, county |

            countyName      = countyName.downcase
            visits          = county['visits']
            quota           = county['quota']
            compensation   = county['compensation']

            "
              <tr>
                <td>#{countyName.capitalize}</td>
                <td>#{visits}</td>
                <td>#{quota}</td>
                <td align='right'>#{('%.2f' % compensation)} KES</td>
              </tr>
            "}.join }
            <tr>
              <td>All</td>
              <td>#{result['visits']['national']['visits']}</td>
              <td>#{result['visits']['national']['quota']}</td>
              <td align='right'>#{('%.2f' % result['visits']['national']['compensation'])} KES</td>
            </tr>
        </tbody>
      </table>
    "

    countyTableHtml = "
      <table id='countyTable'>
        <thead>
          <tr>
            <th>Zone</th>
            <th>TAC Tutor Username</th>
            <th>TAC Tutor First Name</th>
            <th>TAC Tutor Last Name</th>
            <th>Number of classroom visits / valid</a></th>
            <th>Targeted number of classroom visits / valid</th>
            <th>Total Reimbursement</th>
          </tr>
        </thead>
        <tbody>
          #{result['visits']['byCounty'][currentCountyName]['zones'].map{ |zoneName, zone|

              zoneName = zoneName.downcase
              quota = zone['quota']

              if result['users'][currentCountyName][zoneName].length != 0
                result['users'][currentCountyName][zoneName].map { | userName, val |
                  
                  user = result['users']['all'][userName]['data']
                  flagContent = ""

                  if result['users']['all'][userName]['flagged'] == true
                    userLocation = result['users']['all'][userName]['data']['location']
                    userCountyName  = countyTranslate(userLocation['County'].downcase) if !userLocation['County'].nil?
                    userZoneName    = zoneTranslate(userLocation['Zone'].downcase) if !userLocation['Zone'].nil?

                    if (userCountyName == currentCountyName) && (userZoneName == zoneName)

                      visits        = result['users']['all'][userName]['target']['visits']
                      compensation  = result['users']['all'][userName]['target']['compensation']

                    else
                      visits        = result['users']['all'][userName]['other'][currentCountyName][zoneName]['visits']
                      compensation  = result['users']['all'][userName]['other'][currentCountyName][zoneName]['compensation']
                    end

                    flagToolTip = "
                                  <strong>Notice:</strong> This user has completed visits outside their assigned zone. 
                                  <br><br>
                                  <em>Current Assignment:</em> #{userCountyName} > #{userZoneName}
                                  <br><br>
                                  <em>Additional visits have been recorded in:</em>
                                  <ul>
                                    #{
                                      result['users']['all'][userName]['other'].map{ | altCountyName, altCounty |
                                        altCounty.map{ | altZoneName, altZone |
                                          "<li>#{altCountyName} > #{altZoneName}</li>"
                                        }.join()
                                      }.join()
                                    }
                                  </ul>
                                  "

                    flagContent = "<a href='#' onclick='return fase;' title='#{flagToolTip}'><i class='fa fa-flag-o'></i></a>"

                  else
                    
                    visits        = result['users']['all'][userName]['target']['visits']
                    compensation  = result['users']['all'][userName]['target']['compensation']
                  end

                  "
                    <tr> 
                      <td>#{zoneName.capitalize} </td>
                      <td>#{flagContent} #{user['name']}</td>
                      <td>#{user['first']}</td>
                      <td>#{user['last']}</td>
                      <td>#{visits}</td>
                      <td>#{quota}</td>
                      <td align='right'>#{flagContent} #{('%.2f' % compensation)} KES</td>
                    </tr>
                  "
                }.join
              else 
                "
                  <tr> 
                    <td>#{zoneName.capitalize} </td>
                    <td align='center'> --- </td>
                    <td align='center'> --- </td>
                    <td align='center'> --- </td>
                    <td align='center'> --- </td>
                    <td>#{quota}</td>
                    <td align='center'> --- </td>
                  </tr>
                "
              end
            }.join
          }
        </tbody>
      </table>
    "

    zoneTableHtml = "

      <table id='zoneTable'>
        <thead>
          <tr>
            <th>Day</th>
            <th>TAC Tutor</th>
            <th>School Name</th>
            <th>Subject</th>
            <th>Class</th>
            <th>Duration</th>
          </tr>
        </thead>
        <tbody>
          #{ 
          zoneTrips.map{ | trip |
            
            if !groupTimeZone.nil?
              day = Time.at(trip['value']['maxTime'].to_i / 1000).getlocal(groupTimeZone).strftime("%m / %d")
            else 
              day = Time.at(trip['value']['maxTime'].to_i / 1000).strftime("%m / %d")
            end

            subject = subjectLegend[trip['value']['subject']]

            duration = (trip['value']['maxTime'].to_i - trip['value']['minTime'].to_i ) / 1000 / 60

            "
              <tr>
                <td align='center'>#{day}</td>
                <td>#{trip['value']['user']}</td>
                <td>#{trip['value']['school'].capitalize}</td>
                <td>#{subject}</td>
                <td align='center'>#{trip['value']['class']}</td>
                <td align='center'>#{(duration/3600)} Minutes</td>
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
          #map-loading { width: 100%; text-align: center; background-color: #dddd99;}
          #map { clear: both; }
          div.chart { float: left; } 
          h1, h2, h3 
          {
            display: block;
            clear:both;
          }
          .fa-flag-o { color: red; }
          .ui-tooltip {
            font-size: 10px;
          }
        </style>
        <link rel='stylesheet' href='//code.jquery.com/ui/1.11.2/themes/smoothness/jquery-ui.css'>
        <link rel='stylesheet' type='text/css' href='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/css/jquery.dataTables.css'>
        <link href='//maxcdn.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.min.css' rel='stylesheet'>
        <style>
        .ui-tooltip {
            font-size: 12px;
          }
        </style>

        <script src='http://code.jquery.com/jquery-1.11.0.min.js'></script>
        <script src='//code.jquery.com/ui/1.11.2/jquery-ui.js'></script>
        <script src='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/jquery.dataTables.min.js'></script>

        <script>

          
          $(document).ready( function() {
            var nationalTable = $('#nationalTable').dataTable( { iDisplayLength :-1, sDom : 't'});
            var countyTable = $('#countyTable').dataTable( { iDisplayLength :-1, sDom : 't'});
            var zoneTable = $('#zoneTable').dataTable( { iDisplayLength :-1, sDom : 't'});

            $('select').on('change',function() {
              year    = $('#year-select').val().toLowerCase()
              month   = $('#month-select').val().toLowerCase()
              county  = $('#county-select').val().toLowerCase()
              zone  = $('#zone-select').val().toLowerCase()
              
              //Callback for reloading the page - swap commented lines for dev/prod
              document.location = 'http://#{$settings[:host]}#{$settings[:basePath]}/reimbursement/#{group}/#{workflowIds}/'+year+'/'+month+'/'+county+'/'+zone+'.html';
            });

            countyTable.$('a').tooltip({
                content: function () {
                    return $(this).prop('title');
                }
            });

          });

        </script>

      </head>

      <body>
        <h1><img style='vertical-align:middle;' src=\"#{$settings[:basePath]}/images/corner_logo.png\" title=\"Go to main screen.\"> Reimbursement Report - Kenya National Tablet Programme</h1>
  
        <label for='year-select'>Year</label>
        <select id='year-select'>
          <option #{"selected" if year == "2013"}>2013</option>
          <option #{"selected" if year == "2014"}>2014</option>
          <option #{"selected" if year == "2015"}>2015</option>
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

        <h2>
          National Report
          #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
        </h2>
        #{nationalTableHtml}
        <br>
        <hr>
        <br>
        
        
        <label for='county-select'>County</label>
        <select id='county-select'>
          #{
            countyList.map { | countyName |
              "<option #{"selected" if countyName.downcase == currentCountyName}>#{countyName.capitalize}</option>"
            }.join("")
          }
        </select>
        <h2>
          #{currentCountyName.capitalize} County Report
          #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
        </h2>
        #{countyTableHtml}
        <br>
        <hr>
        <br>
        
        
        <label for='zone-select'>Zone</label>
        <select id='zone-select'>
          #{
            zoneList.map { | zoneName |
              "<option #{"selected" if zoneName.downcase == currentZoneName}>#{zoneName.capitalize}</option>"
            }
            }.join("")
          }
        </select>
        <h2>
          #{currentZoneName.capitalize} Zone Report
          #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
        </h2>
        #{zoneTableHtml}
        </body>
      </html>
      "
    
    return html


  end # of report

  def invalidReport
    html =  "
    <html>
      <head>
        <style>
          body{font-family:Helvetica;}
          #map-loading { width: 100%; text-align: center; background-color: #dddd99;}
          #map { clear: both; }
          div.chart { float: left; } 
          h1, h2, h3 
          {
            display: block;
            clear:both;
          }
          .fa-flag-o { color: red; }
          .ui-tooltip {
            font-size: 10px;
          }
        </style>
        <link href='//maxcdn.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.min.css' rel='stylesheet'>
        <style>
        .ui-tooltip {
            font-size: 12px;
          }
        </style>
      </head>

      <body>
        <h1><img style='vertical-align:middle;' src=\"#{$settings[:basePath]}/images/corner_logo.png\" title=\"Tangerine Logo.\"> Invalid Report</h1>
        </body>
      </html>
      "
    
    return html
  end

end
