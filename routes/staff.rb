#encoding: utf-8
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

  get '/staff/:group/:year/:month/:endMonth/:county/:zone.:format?' do | group, year, month, endMonth, county, zone, format |

    format = "html" unless format == "json"
    
    countyId = county
    zoneId   = zone

    requestId = SecureRandom.base64

    TRIP_KEY_CHUNK_SIZE = 500

    couch = Couch.new({
      :host      => $settings[:dbHost],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    #
    # get Group settings
    #
    groupSettings = couch.getRequest({ :doc => 'settings', :parseJson => true })
    groupTimeZone = groupSettings['timeZone'] 

    #
    # Get quota information
    # 
    begin
      reportSettings = couch.getRequest({ :doc => "report-aggregate-settings", :parseJson => true })
      result  = couch.getRequest({ :doc => "report-aggregate-year#{year}month#{month}", :parseJson => true })
      results = {}
      months  = []
      #loop on months selected
      (month..endMonth).each { |mnt|
        months.push mnt
        results[mnt] = couch.getRequest({ :doc => "report-aggregate-year#{year}month#{mnt}", :parseJson => true })
      }

    rescue => e
      # the doc doesn't already exist
      puts e
      return invalidReport()
    end

    
    currentCountyId       = nil
    currentCounty         = nil
    currentCountyName     = nil
    
    currentZoneId         = nil
    currentZoneName       = nil
    currentZone           = nil 

   
    #ensure that the county in the URL is valid - if not, select the first
    if result['staff']['byCounty'][countyId].nil?
      result['staff']['byCounty'].find { |countyId, county|
        currentCountyId   = countyId
        currentCounty     = county
        currentCountyName = county['name']
        true
      }
    else 
      currentCountyId   = countyId
      currentCounty     = result['staff']['byCounty'][countyId]
      currentCountyName = currentCounty['name']
    end

    #ensure that the zone in the URL is valid - if not, select the first
    if result['staff']['byCounty'][countyId]['zones'][zoneId].nil?
        result['staff']['byCounty'][countyId]['zones'].find { |zoneId, zone|
          currentZoneId   = zoneId
          currentZone     = zone
          currentZoneName = zone['name']
          true
        }
    else 
        currentZoneId   = zoneId
        currentZone     = result['staff']['byCounty'][countyId]['zones'][zoneId]
        currentZoneName = currentZone['name']
    end

    #retrieve a county list for the select and sort it


    #county level data
    row = 0
    totalNationalGpsVisits = 0
    totalNationaVisits     = 0

    #national totals
    months.each{ | m |
      totalNationalGpsVisits += results[m]['staff']['national']['gpsvisits']
      totalNationaVisits     += results[m]['staff']['national']['visits']
    }
    countyTable = "<table class='county-table'>
        <thead>
          <tr>
            <th>County</th>
            <th class='custSort'>Number of classroom visits - With GPS<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            <th class='custSort'>Number of classroom visits - Without GPS<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
          </tr>
        </thead>
        <tbody>
          
          #{ result['staff']['byCounty'].map{ | countyId, county |

            countyName      = county['name']
            gpsvisits       = county['gpsvisits']
            visits          = county['visits']
            quota           = county['quota']
            sampleTotal     = 0

            totalGpsVisits = 0
            totalVisits    = 0
            
            months.each{ | m |
              totalGpsVisits += results[m]['staff']['byCounty'][countyId]['gpsvisits']
              totalVisits    += results[m]['staff']['byCounty'][countyId]['visits']
            }

            "
              <tr>
                <td>#{titleize(countyName)}</td>
                <td>#{totalGpsVisits} ( #{percentage( quota, totalGpsVisits)}% )</td>
                <td>#{totalVisits} ( #{percentage( quota, totalVisits )}% )</td>
              </tr>
            "}.join }
            <tr>
              <td>All</td>
              <td>#{totalNationalGpsVisits} ( #{percentage( result['staff']['national']['quota'], totalNationalGpsVisits )}% )</td>
              <td>#{totalNationaVisits} ( #{percentage( result['staff']['national']['quota'], totalNationaVisits )}% )</td>
            </tr>
        </tbody>
      </table>"


    #build user list from selected periods
    users = []
    months.each{ | m |
       results[m]['staff']['byCounty'][currentCountyId]['users'].map{ | userId, user |
          users.push userId
       }
    }
    
    users = users.uniq

    userCountyTable = "
      <label for='county-select'>County</label>
        <select id='county-select'>
          #{
            orderedCounties = result['staff']['byCounty'].sort_by{ |countyId, county| county['name'] }
            orderedCounties.map{ | countyId, county |
              "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
            }.join("")
          }
        </select>
      <table class='county-table'>
        <thead>
          <tr>
            <th>Staff name</th>
            <th>Number of classroom visits - With GPS<br>
            <small>( Percentage of Target Visits)</small></th>
            <th>Number of classroom visits - Without GPS<br>
            <small>( Percentage of Target Visits)</small></th>
          </tr>
        </thead>
        <tbody>

          #{ users.map{ | userId |
            
            quota           = result['staff']['byCounty'][currentCountyId]['quota']
          
            totalGpsVisits = 0
            totalVisits    = 0
            
            months.each{ | m |

              if !results[m]['staff']['byCounty'][currentCountyId]['users'][userId].nil?
                totalGpsVisits += results[m]['staff']['byCounty'][currentCountyId]['users'][userId]['gpsvisits']
                totalVisits    += results[m]['staff']['byCounty'][currentCountyId]['users'][userId]['visits']

              end              
              
            }

            "
              <tr>
                <td>#{titleize(userId)}</td>
                <td>#{totalGpsVisits} ( #{percentage( quota, totalGpsVisits )}% )</td>
                <td>#{totalVisits} ( #{percentage( quota, totalVisits)}% )</td>
              </tr>
            "}.join }
            
        </tbody>
      </table>"

    zoneTable = "<table class='county-table'>
        <thead>
          <tr>
            <th>Zone</th>
            <th>Number of classroom visits - With GPS<br>
            <small>( Percentage of Target Visits)</small></th>
            <th>Number of classroom visits - Without GPS<br>
            <small>( Percentage of Target Visits)</small></th>
          </tr>
        </thead>
        <tbody>
          #{ result['staff']['byCounty'][currentCountyId]['zones'].map{ | zoneId, zone |
            visits          = zone['visits']
            gpsvisits       = zone['gpsvisits']
            quota           = result['staff']['byCounty'][currentCountyId]['quota']
            zoneName        = zone['name']

            totalGpsVisits = 0
            totalVisits    = 0
            
            months.each{ | m |

              totalGpsVisits += results[m]['staff']['byCounty'][currentCountyId]['zones'][zoneId]['gpsvisits']
              totalVisits    += results[m]['staff']['byCounty'][currentCountyId]['zones'][zoneId]['visits']  
              
              
            }

            "
              <tr>
                <td>#{titleize(zoneName)}</td>
                <td>#{totalGpsVisits} ( #{percentage( quota, totalGpsVisits )}% )</td>
                <td>#{totalVisits} ( #{percentage( quota, totalVisits )}% )</td>
              </tr>
            "}.join }
            
        </tbody>
      </table>"

    schoolsTable = "<table class='county-table'>
        <thead>
          <tr>
            <th>School</th>
            <th>Number of classroom visits - With GPS</th>
            <th>Number of classroom visits - Without GPS</th>
          </tr>
        </thead>
        <tbody>
          #{ result['staff']['byCounty'][currentCountyId]['zones'][currentZoneId]['schools'].map{ | schoolId, school |
            visits          = school['visits']
            gpsvisits       = school['gpsvisits']
            quota           = result['staff']['byCounty'][currentCountyId]['quota']
            schoolName      = school['name']
            
            totalGpsVisits = 0
            totalVisits    = 0

            months.each{ | m |

              if ! results[m]['staff']['byCounty'][currentCountyId]['zones'][currentZoneId]['schools'][schoolId].nil?
                totalGpsVisits += results[m]['staff']['byCounty'][currentCountyId]['zones'][currentZoneId]['schools'][schoolId]['gpsvisits']
                totalVisits    += results[m]['staff']['byCounty'][currentCountyId]['zones'][currentZoneId]['schools'][schoolId]['visits']  
              end
              
            }

            "
              <tr>
               
                <td>#{titleize(schoolName)}</td>
                <td>#{totalGpsVisits}</td>
                <td>#{totalVisits}</td>
              </tr>
            "}.join }
              
        </tbody>
      </table>"

    countyTab = "<h2>Staff Report (#{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]} - #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][endMonth.to_i]})</h2>
      <hr>
      #{countyTable}
      <br>
       <h2>
        #{titleize(currentCountyName)} County Report
      </h2>
      #{userCountyTable}
      <br>
      #{zoneTable}
      <br>
      <div id='staff-map-loading'>Please wait. Data loading...</div>
      <div id='staff-map' style='height: 400px'></div>
      <br>
      "

    userTab = "<h2>#{titleize(currentCountyName)} - #{titleize(currentZoneName)} Report (#{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]} - #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][endMonth.to_i]})</h2>
      <hr>
      <label for='school-county-select'>County</label>
        <select id='school-county-select'>
          #{
            orderedCounties = result['staff']['byCounty'].sort_by{ |countyId, county| county['name'] }
            orderedCounties.map{ | countyId, county |
              "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
            }.join("")
          }
        </select>&nbsp;
        <label for='school-zone-select'>Zone</label>
        <select id='school-zone-select'>
          #{
            orderedCounties = result['staff']['byCounty'][currentCountyId]['zones'].sort_by{ |zoneId, zone| zone['name'] }
            orderedCounties.map{ | zoneId, zone |
              "<option value='#{zoneId}' #{"selected" if zoneId == currentZoneId}>#{titleize(zone['name'])}</option>"
            }.join("")
          }
        </select>
      <br>
      #{schoolsTable}"

    html =  "<html>
      <head>
        <link rel='stylesheet' type='text/css' href='#{$settings[:basePath]}/css/report.css'>
        <link rel='stylesheet' type='text/css' href='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/css/jquery.dataTables.css'>
        <link rel='stylesheet' type='text/css' href='http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.css'>
        <link rel='stylesheet' type='text/css' href='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/MarkerCluster.css'>
        <link rel='stylesheet' type='text/css' href='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/MarkerCluster.Default.css'>
        

        <script src='http://cdnjs.cloudflare.com/ajax/libs/moment.js/2.9.0/moment.min.js'></script>

        <script src='#{$settings[:basePath]}/javascript/base64.js'></script>
        <script src='http://code.jquery.com/jquery-1.11.0.min.js'></script>
        <script src='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/jquery.dataTables.min.js'></script>
        <script src='http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.js'></script>
        <script src='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/leaflet.markercluster.js'></script>
        <script src='#{$settings[:basePath]}/javascript/leaflet/leaflet-providers.js'></script>
        <script src='#{$settings[:basePath]}/javascript/leaflet/leaflet.ajax.min.js'></script>

        <script src='http://d3js.org/d3.v3.min.js'></script>
        <script src='http://dimplejs.org/dist/dimple.v2.0.0.min.js'></script>
        <script src='http://d3js.org/queue.v1.min.js'></script>

        <script>
          var base = 'http://#{$settings[:host]}#{$settings[:basePath]}/'; // will need to update this for live development

          updateMap = function() {

            if ( window.markers == null || window.map == null || window.geoJsonLayer == null ) { return; }

            window.markers.addLayer(window.geoJsonLayer);
            window.map.addLayer(window.markers);
            $('#map-loading').hide();

          };

          var mapDataURL = new Array();
          mapDataURL['current'] = new Array();
          mapDataURL['all'] = new Array();

          mapDataURL['current']
          #{
            months.map{ | m |
              "mapDataURL['current'].push(base+'reportData/#{group}/report-aggregate-geo-year#{year.to_i}month#{m.to_i}-#{currentCountyId}.geojson');
              "
            }.join("")
          }
          
          mapDataURL['all']
          #{
            result['staff']['byCounty'].map{ | countyId, county |
              "mapDataURL['all'].push(base+'reportData/#{group}/report-aggregate-geo-year#{year.to_i}month#{month.to_i}-#{countyId}.geojson');
              "
            }.join("")
          }
          
          swapMapData = function(){
            window.geoJsonLayer.refresh(mapDataURL['all']);
            $('#map-loading').show();
          }

          //init a datatables advanced sort plugin
          jQuery.extend( jQuery.fn.dataTableExt.oSort, {
                'num-html-pre': function ( a ) {
                    var x = String(a).replace( /<[\\s\\S]*?>/g, '' );
                    if(String(a).indexOf('no data')!= -1){
                      x = 0;
                    }
                    return parseFloat( x );
                },
             
                'num-html-asc': function ( a, b ) {
                    return ((a < b) ? -1 : ((a > b) ? 1 : 0));
                },
             
                'num-html-desc': function ( a, b ) {
                    return ((a < b) ? 1 : ((a > b) ? -1 : 0));
                }
          });

          L.Icon.Default.imagePath = 'http://ntp.tangerinecentral.org/images/leaflet'
          var pageMaps = {}
          var mapControls = {
            
            staff: {
              osm: new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                minZom: 1,
                maxZoom: 12,
                attribution: 'Map data © OpenStreetMap contributors'
              }),
              layerControl: L.control.layers.provided(['OpenStreetMap.Mapnik','Stamen.Watercolor']),
              markers: L.markerClusterGroup(),
              layerGeoJsonFilter: function(feature, layer){
                return (feature.role === 'staff');
              }
            }
          };


          var layerOnEachFeature = function(feature, layer){
            var html = '';
            if (feature != null && feature.properties != null && feature.properties.length != null ){
              feature.properties.forEach(function(cell){
                if(cell.label != 'role'){
                  html += '<b>' + cell.label + '</b> ' + cell.value + '<br>';
                }
              });
            }
            
            layer.bindPopup( html );
          };

          $(document).ready( function() {
             
              /***********
              **
              **   Init Custom Data Tables
              **
              ************/
              //init display for table
              
              $('table.county-table').dataTable( { 
                iDisplayLength :-1, 
                sDom : 't'
              });

              var currCounty = '#{countyId}';
              var zone = '#{zoneId}';

              $('#year-select,#month-select,#end-month-select').on('change',function() {
                reloadReport();
              });

              $('#county-select').on('change',function() {
                currCounty = $('#county-select').val()
                
                reloadReport();
              });

              $('#school-county-select').on('change',function() {
                currCounty = $('#school-county-select').val()
                
                reloadReport();
              });

              $('#school-zone-select').on('change',function() {
                  zone = $('#school-zone-select').val()
                  reloadReport();
              });

              function reloadReport(){
                year    = $('#year-select').val().toLowerCase()
                month   = $('#month-select').val().toLowerCase()
                endMonth   = $('#end-month-select').val().toLowerCase()
                console.log(currCounty, zone, year, month)
                document.location = 'http://#{$settings[:host]}#{$settings[:basePath]}/staff/#{group}/'+year+'/'+month+'/'+endMonth+'/'+currCounty+'/'+zone+'.html';
              }

            /***********
            **
            **   Init Leaflet Maps
            **
            ************/

            
            window.markers = L.markerClusterGroup();
            
            pageMaps.staff = new L.Map('staff-map');

            //----------- MATHS MAP CONFIG -------------------------
            pageMaps.staff.addLayer(mapControls.staff.osm);
            pageMaps.staff.setView(new L.LatLng(0, 35), 6);
            mapControls.staff.layerControl.addTo(pageMaps.staff);
            mapControls.staff.geoJsonLayer = new L.GeoJSON.AJAX(mapDataURL['current'], {
              onEachFeature: layerOnEachFeature,
              filter: mapControls.staff.layerGeoJsonFilter
            });
            mapControls.staff.geoJsonLayer.on('data:loaded', function(){
              if ( mapControls.staff.markers == null || pageMaps.staff == null || mapControls.staff.geoJsonLayer == null ) { return; }
              mapControls.staff.markers.addLayer(mapControls.staff.geoJsonLayer);
              pageMaps.staff.addLayer(mapControls.staff.markers);
              $('#staff-map-loading').hide();
            });
            $('#staff-view-all-btn').on('click', function(event){
              mapControls.staff.geoJsonLayer.refresh(mapDataURL['all']);
              $('#staff-map-loading').show();
              $('#staff-view-all-btn').hide();
            });

            /*
            var
              layerControl,
              osm
            ;

            window.map = new L.Map('map');

            osm = new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              minZom: 1,
              maxZoom: 12,
              attribution: 'Map data © OpenStreetMap contributors'
            });

            map.addLayer(osm);
            map.setView(new L.LatLng(0, 35), 6);

            layerControl = L.control.layers.provided([
              'OpenStreetMap.Mapnik',
              'Stamen.Watercolor'
            ]).addTo(map);

            window.markers = L.markerClusterGroup();
            */
            // ready map data

            //var geojson = {
            //  'type'     : 'FeatureCollection',
            //  'features' : {} //{#geojson.to_json}
            //};


            /*
            window.geoJsonLayer = new L.GeoJSON.AJAX(mapDataURL['current'], {
              onEachFeature: function( feature, layer ) {
                var html = '';
            
                if (feature != null && feature.properties != null && feature.properties.length != null )
                {
                  feature.properties.forEach(function(cell){
                    html += '<b>' + cell.label + '</b> ' + cell.value + '<br>';
                  });
                }
                
                layer.bindPopup( html );
              }
            });

            window.geoJsonLayer.on('data:loaded', window.updateMap);
            */
            //window.geoJsonLayer = L.geoJson( geojson, {
            //  onEachFeature: function( feature, layer ) {
            //    var html = '';
            //
            //    if (feature != null && feature.properties != null && feature.properties.length != null )
            //    {
            //      feature.properties.forEach(function(cell){
            //        html += '<b>' + cell.label + '</b> ' + cell.value + '<br>';
            //      });
            //    }
            //    
            //    layer.bindPopup( html );
            //  } // onEachFeature
            //}); // geoJson

          });
        

        </script>

      </head>

      <body>
        <h1><img style='vertical-align:middle;' src=\"#{$settings[:basePath]}/images/corner_logo.png\" title=\"Go to main screen.\"> TUSOME</h1>
  
        <label for='year-select'>Year</label>
        <select id='year-select'>
          <option #{"selected" if year == "2014"}>2014</option>
          <option #{"selected" if year == "2015"}>2015</option>
          <option #{"selected" if year == "2016"}>2016</option>
          <option #{"selected" if year == "2017"}>2017</option>
        </select>

        <label for='month-select'>Month</label>
        <select id='month-select'>
          <option value='1'  #{"selected" if month == "1"}>Jan</option>
          <option value='2'  #{"selected" if month == "2"}>Feb</option>
          <option value='3'  #{"selected" if month == "3"}>Mar</option>
          <!--<option value='4'  #{"selected" if month == "4"}>Apr</option>-->
          <option value='5'  #{"selected" if month == "5"}>May</option>
          <option value='6'  #{"selected" if month == "6"}>Jun</option>
          <option value='7'  #{"selected" if month == "7"}>Jul</option>
          <!--<option value='8'  #{"selected" if month == "8"}>Aug</option>-->
          <option value='9'  #{"selected" if month == "9"}>Sep</option>
          <option value='10' #{"selected" if month == "10"}>Oct</option>
          <option value='11' #{"selected" if month == "11"}>Nov</option>
          <!--<option value='12' #{"selected" if month == "12"}>Dec</option>-->
        </select>

        <label for='end-month-select'>Month</label>
        <select id='end-month-select'>
          <option value='1'  #{"selected" if endMonth == "1"}>Jan</option>
          <option value='2'  #{"selected" if endMonth == "2"}>Feb</option>
          <option value='3'  #{"selected" if endMonth == "3"}>Mar</option>
          <!--<option value='4'  #{"selected" if endMonth == "4"}>Apr</option>-->
          <option value='5'  #{"selected" if endMonth == "5"}>May</option>
          <option value='6'  #{"selected" if endMonth == "6"}>Jun</option>
          <option value='7'  #{"selected" if endMonth == "7"}>Jul</option>
          <!--<option value='8'  #{"selected" if endMonth == "8"}>Aug</option>-->
          <option value='9'  #{"selected" if endMonth == "9"}>Sep</option>
          <option value='10' #{"selected" if endMonth == "10"}>Oct</option>
          <option value='11' #{"selected" if endMonth == "11"}>Nov</option>
          <!--<option value='12' #{"selected" if endMonth == "12"}>Dec</option>-->
        </select>

          <div class='tab_container'>
            <div id='tab-tutor' class='tab first selected' data-id='tutor'>County</div>
            <div id='tab-user' class='tab' data-id='user'>Schools</div>
            <section id='panel-tutor' class='tab-panel' style=''>
              #{countyTab}
            </section>
            <section id='panel-user' class='tab-panel' style='display:none;'>
              #{userTab}
            </section>
          </div>


          <script>
          /*****
          **  Setup and Manage Tabs
          ******/
          $('.tab').on('click', handleTabClick);

          function handleTabClick(event){
            var tabId = $(event.target).attr('data-id');
            displayTab(tabId);
            
            event.preventDefault();
            window.location.hash = '#'+tabId;
          }

          function forceTabSelect(tabId){
            if( $('#tab-'+tabId).length ){
              displayTab(tabId)
            } else {
              displayTab('tutor')
            }
          }

          function displayTab(tabId){
            $('.tab').removeClass('selected');
            $('.tab-panel').hide();

            $('#tab-'+tabId).addClass('selected');
            $('#panel-'+tabId).show();
            
            if(typeof pageMaps[tabId] !== 'undefined'){
              pageMaps[tabId].invalidateSize();
            }
            
          }
        </script>
        </body>
        </html>"

    
    return html


  end # of report

end
