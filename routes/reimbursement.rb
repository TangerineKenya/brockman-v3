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
  #url
  get '/reimbursement/:group/:year/:month/:county/:zone' do |group,year,month,county,zone|
    
    #default vals 
    countyId = county
    zoneId = zone

    #db connection
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
      # Get information
      # 
      begin
        reportSettings = couch.getRequest({ :doc => "report-aggregate-settings", :parseJson => true })
        result = couch.getRequest({ :doc => "report-aggregate-year#{year}month#{month}", :parseJson => true })
     
      rescue => e
        # the doc doesn't already exist
        puts e
        #return invalidReport()
      end

      currentCountyId       = nil
      currentCountyName     = nil
      currentCounty       = nil

      currentZoneId         = nil
      currentZoneName       = nil
      currentZone       = nil 

      #ensure that the county in the URL is valid - if not, select the first
      if result['visits']['byCounty'][countyId].nil?
        result['visits']['byCounty'].find { |countyId, county|
          currentCountyId   = countyId
          currentCounty     = county
          currentCountyName = county['name']
          true
        }
      else 
        currentCountyId   = countyId
        currentCounty     = result['visits']['byCounty'][countyId]
        currentCountyName = currentCounty['name']
      end
      #ensure that the zone in the URL is valid - if not, select the first
      if result['visits']['byCounty'][countyId]['zones'][zoneId].nil?
        result['visits']['byCounty'][countyId]['zones'].find { |zoneId, zone|
          currentZoneId   = zoneId
          currentZone     = zone
          currentZoneName = zone['name']
          true
        }
      else 
        currentZoneId   = zoneId
        currentZone     = result['visits']['byCounty'][countyId]['zones'][zoneId]
        currentZoneName = currentZone['name']
      end

      nationalTableHtml = "
          <table class='national-table'>
            <thead>
              <tr>
                <th>County</th>
                <th>Number of classroom visits / valid</th>
                <th>Number of Teachers</th>
                <th>Total Reimbursement</th>
              </tr>
            </thead>
            <tbody>
              #{ result['visits']['byCounty'].map{ | countyId, county |

                countyName      = county['name'].downcase
                visits          = county['visits']
                quota           = county['numTeachers']
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
                  <td>#{result['visits']['national']['numTeachers']}</td>
                  <td align='right'>#{('%.2f' % result['visits']['national']['compensation'])} KES</td>
                </tr>
            </tbody>
          </table>
        "

    countyTableHtml = "
      <table class='county-table'>
        <thead>
          <tr>
            <th>Zone</th>
            <th>TAC Tutor Username</th>
            <th>M-Pesa Number</th>
            <th>Number of classroom visits / valid</a></th>
            <th>Number of Teachers</th>
            <th>Total Reimbursement</th>
          </tr>
        </thead>
        <tbody>
          #{result['visits']['byCounty'][currentCountyId]['zones'].map{ |zoneId, zone|

              zoneName = zone['name'].downcase
              quota = zone['numTeachers']

              if result['users']['all'].length != 0
                result['users']['all'].map{ | username,  user |
               
                if !result['users']['all'][username]['other'][currentCountyId].nil? && !result['users']['all'][username]['other'][currentCountyId][zoneId].nil?
                  phone = result['users']['all'][username]['data']['Mpesa']
                  visits        = result['users']['all'][username]['other'][currentCountyId][zoneId]['visits']
                  compensation  = result['users']['all'][username]['other'][currentCountyId][zoneId]['compensation']
                           
                   "
                          <tr> 
                            <td>#{zoneName.capitalize} </td>
                            <td>#{username.capitalize}</td>
                            <td>#{phone}</td>
                            <td>#{visits}</td>
                            <td>#{quota}</td>
                            <td align='right'>#{('%.2f' % compensation)} KES</td>
                          </tr>
                        "
                end
                }.join
              else 
                
              end
            }.join
          }
        </tbody>
      </table>
    "


  zoneTableHtml = ""

    html = "<html>
          <head>
            <title>Reimbursement Report </title>
            <link rel='stylesheet' type='text/css' href='#{$settings[:basePath]}/css/report.css'>
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
                  $('table.national-table').dataTable( { 
                      iDisplayLength :-1, 
                      sDom : 't'
                    });

                    $('table.county-table').dataTable( { 
                      iDisplayLength :-1, 
                      sDom : 't'
                    });

                    $('table.zone-table').dataTable( { 
                      iDisplayLength :-1, 
                      sDom : 't'
                    });

                  var county = '#{countyId}';
                  var zone = '#{zoneId}';

                  $('#year-select,#month-select').on('change',function() {
                        reloadReport();
                      });

                  $('#county-select').on('change',function() {
                        county = $('#county-select').val()
                        reloadReport();
                      });

                  $('#zone-select').on('change',function() {
                        zone = $('#zone-select').val()
                        reloadReport();
                  });

                  function reloadReport(){
                      year    = $('#year-select').val().toLowerCase()
                        month   = $('#month-select').val().toLowerCase()
                      document.location = 'http://#{$settings[:host]}#{$settings[:basePath]}/reimbursement/#{group}/'+year+'/'+month+'/'+county+'/'+zone;
                  }
              });
                </script>
          </head>
          <body>
            <h1><img style='vertical-align:middle;' src=\"#{$settings[:basePath]}/images/corner_logo.png\" title=\"Go to main screen.\">  Reimbursement Report</h1>
            <label for='year-select'>Year</label>
                <select id='year-select'>
                  <option #{"selected" if year == "2014"}>2014</option>
                  <option #{"selected" if year == "2015"}>2015</option>
                  <option #{"selected" if year == "2016"}>2016</option>
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
                    orderedCounties = result['visits']['byCounty'].sort_by{ |countyId, county| county['name'] }
                    orderedCounties.map{ | countyId, county |
                      "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
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

                
          </body>
        </html>" 

    return html
  end
  #end report
end