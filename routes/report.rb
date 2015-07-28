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

  get '/report/:group/:workflowIds/:year/:month/:county.:format?' do | group, workflowIds, year, month, county, format |

    format = "html" unless format == "json"
    
    safeCounty = county
    
    begin
     county = Base64.urlsafe_decode64 county
    rescue
     county = "baringo"
    end


    requestId = SecureRandom.base64

    TRIP_KEY_CHUNK_SIZE = 500

    couch = Couch.new({
      :host      => $settings[:dbHost],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    subjectLegend = { "english_word" => "English", "word" => "Kiswahili", "operation" => "Maths" } 

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
      result = couch.getRequest({ :doc => "report-aggregate-year#{year}month#{month}", :parseJson => true })
    rescue => e
      # the doc doesn't already exist
      puts e
      return invalidReport()
    end

    currentCounty         = nil
    currentCountyName     = county.downcase #params[:county].downcase

   
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

    #retrieve a county list for the select and sort it
    countyList = []
    result['visits']['byCounty'].map { |countyName, county| countyList.push countyName }
    countyList.sort!

    chartJs = "
      
      var base = 'http://#{$settings[:host]}#{$settings[:basePath]}/'; // will need to update this for live development

      // called on document ready
      var initChart = function()
      {
        var TREND_MONTHS = 3;  // number of months to try to pull into trend
        var month        = #{month.to_i};  // starting month
        var year         = #{year.to_i}; // starting year
        var safeCounty  = '#{safeCounty}';

        var reportMonth = moment(new Date(year, month, 1));
      
        var quotas_link = '/#{group}/geography-quotas';



        dates[TREND_MONTHS]       = { month:month, year:year};
        dates[TREND_MONTHS].link  = base+'reportData/#{group}/report-aggregate-year#{year.to_i}month#{month.to_i}.json';


        // create links for trends by month
        for ( var i = TREND_MONTHS-1; i > 0; i-- ) {
          tgtMonth      = reportMonth.clone().subtract((TREND_MONTHS - i + 1), 'months');
          dates[i]      = { month:tgtMonth.get('month')+1, year:tgtMonth.get('year')};
          dates[i].link = base+'reportData/#{group}/report-aggregate-year'+dates[i].year+'month'+dates[i].month +'.json';
          console.log('generating date' + i)
        }
        
        // call the links in a queue and then execute the last function
        var q = queue();
        for(var j=1;j<dates.length;j++)
        {
          q.defer(d3.json,dates[j].link);
        }
        q.await(buildReportCharts);
      }



      var dataset = Array()
      var dates = Array();
      var months = {
        1:'January',
        2:'February',
        3:'March',
        4:'April',
        5:'May',
        6:'June',
        7:'July',
        8:'August',
        9:'September',
        10:'October',
        11:'November',
        12:'December'
      };  
      
      function buildReportCharts()
      {
        console.log(arguments);
        // sort out the responses and add the data to the corresponding dates array
        for(var j=arguments.length-1;j>=0;j--)
        {
          if(j==0)
          {
            var error = arguments[j];
          }
          else
          {
            dates[j].data = arguments[j]; // need to change for live when not using a proxy
          }
        }
        
        var quota = null //{geography.to_json};

        // loop over data and build d3 friendly dataset 
        dates.forEach(function(el){
          var tmpset = Array();
	  console.log(el);
          for(var county in el.data.visits.byCounty)
          {
            var tmp = {
              County   : capitalize(county),
              MonthInt : el.month,
              Year     : el.year,
              Month    : months[el.month]
            };
            
            tmp['English Score'] = safeRead(el.data.visits.byCounty[county].fluency,'english_word','sum')/safeRead(el.data.visits.byCounty[county].fluency,'english_word','size');
            if(isNaN(tmp['English Score'])) { delete tmp['English Score'] };

            tmp['Kiswahili Score'] = safeRead(el.data.visits.byCounty[county].fluency,'word','sum')/safeRead(el.data.visits.byCounty[county].fluency,'word','size');
            if(isNaN(tmp['Kiswahili Score'])) { delete tmp['Kiswahili Score'] };

            //tmp['Math Score'] = safeRead(el.data.visits.byCounty[county].fluency,'operation','sum')/safeRead(el.data.visits.byCounty[county].fluency,'operation','size');
            //if(isNaN(tmp['Math Score'])) { delete tmp['Math Score'] };

            var countyVisits = safeRead(el.data.visits.byCounty[county], 'visits');
            var countyQuota = safeRead(el.data.visits.byCounty[county],'quota');
            if (countyVisits == 0 || countyQuota == 0)
            {
              tmp['Visit Attainment'] = 0;
            } else {
              tmp['Visit Attainment'] = countyVisits / countyQuota * 100;
            }
            
            if(isNaN(tmp['Visit Attainment'])) delete tmp['Visit Attainment'];
                          
            dataset.push(tmp);
          }
        })
        
        // Build the charts. 
        addChart('English Score', 'English Score', 'Correct Items Per Minute');
        addChart('Kiswahili Score', 'Kiswahili Score', 'Correct Items Per Minute');
        //addChart('Math Score', 'Maths Score', 'Correct Items Per Minute');
        addChart('Visit Attainment', 'TAC Tutor Classroom Observations','Percentage');
        $('#charts-loading').remove()

      }     

    
      function addChart(variable, title, yaxis)
      {
        // create the element that the chart lives in
        var domid = (new Date()).getTime();
        $('#charts').append('<div class=\"chart\"><h2>'+title+'</h2><div id=\"chartContainer'+domid+'\" /></div>');

        // start building chart object to pass to render function
        chartObject = new Object();
        chartObject.container = '#chartContainer'+domid;
        chartObject.height = 300;
        chartObject.width = 550;
        chartObject.data =  dataset;
        
        chartObject.plot = function(chart){

          // setup x, y and series
          var x = chart.addCategoryAxis('x', ['County','Month']);
          x.addOrderRule('County');
          x.addGroupOrderRule('MonthInt');

          var y = chart.addMeasureAxis('y', variable);

          var series = chart.addSeries(['Month'], dimple.plot.bar);
          series.addOrderRule('MonthInt');
          series.clusterBarGap = 0;
          
          // add the legend
          chart.addLegend(chartObject.width-100, chartObject.height/2-25, 100,  150, 'left');
        };
        
        // titles for x and y axis
        chartObject.xAxis = 'County';
        chartObject.yAxis = yaxis;
        
        // show hover tooltips
        chartObject.showHover = true;
        buildChart(chartObject);
      }
      
      function buildChart(chart)
      {
        var svg = dimple.newSvg(chart.container, chart.width, chart.height);

        //set white background for svg - helps with conversion to png
        svg.append('rect').attr('x', 0).attr('y', 0).attr('width', chart.width).attr('height', chart.height).attr('fill', 'white');
          
        var dimpleChart = new dimple.chart(svg, chart.data);
        dimpleChart.setBounds(50, 50, chart.width-150, chart.height-100);
        chartObject.plot(dimpleChart);

        if(!chart.showHover)
        {
          dimpleChart.series[0].addEventHandler('mouseover', function(){});
          dimpleChart.series[0].addEventHandler('mouseout', function(){});
        }

        dimpleChart.draw();
        
        // x axis title and redraw bottom line after removing tick marks
        dimpleChart.axes[0].titleShape.text(chartObject.xAxis).style({'font-size':'11px', 'stroke': '#555555', 'stroke-width':'0.2px'});
        dimpleChart.axes[0].shapes.selectAll('line').remove();
        dimpleChart.axes[0].shapes.selectAll('path').attr('d','M50,1V0H'+String(chart.width-80)+'V1').style('stroke','#555555');
        if(!dimpleChart.axes[1].hidden)
        {
          // update y axis
          dimpleChart.axes[1].titleShape.text(chartObject.yAxis).style({'font-size':'11px', 'stroke': '#555555', 'stroke-width':'0.2px'});
          dimpleChart.axes[1].gridlineShapes.selectAll('line').remove();
        }
        return dimpleChart;
      }
      
      function capitalize(string)
      {
          return string.charAt(0).toUpperCase() + string.slice(1);
      }
      
      //
      // Usage... for a nested structure
      // var test = {
      //    nested: {
      //      value: 'Read Correctly'
      //   }
      // };
      // safeRead(test, 'nested', 'value');  // returns 'Read Correctly'
      // safeRead(test, 'missing', 'value'); // returns ''
      //
      var safeRead = function() {
        var current, formatProperty, obj, prop, props, val, _i, _len;

        obj = arguments[0], props = 2 <= arguments.length ? [].slice.call(arguments, 1) : [];

        read = function(obj, prop) {
          if ((obj != null ? obj[prop] : void 0) == null) {
            return;
          }
          return obj[prop];
        };

        current = obj; 
        for (_i = 0, _len = props.length; _i < _len; _i++) {
          prop = props[_i];

          if (val = read(current, prop)) {
            current = val;
          } else {
            return '';
          }
        }
        return current;
      };


    "

    
    
    


    row = 0
    countyTableHtml = "
      <table>
        <thead>
          <tr>
            <th>County</th>
            <th>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a></th>
            <th>Targeted number of classroom visits<a href='#footer-note-2'><sup>[2]</sup></a></th>
            #{reportSettings['fluency']['subjects'].map{ | subject |
              "<th>#{subjectLegend[subject]}<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | countyName, county |

            countyName      = countyName.downcase
            visits          = county['visits']
            quota           = county['quota']
            sampleTotal     = 0

            "
              <tr>
                <td>#{countyName.capitalize}</td>
                <td>#{visits}</td>
                <td>#{quota}</td>
                #{reportSettings['fluency']['subjects'].map{ | subject |
                  #ensure that there, at minimum, a fluency category for the county
                  sample = county['fluency'][subject]
                  if sample.nil?
                    average = "no data"
                  else
                    if sample && sample['size'] != 0 && sample['sum'] != 0
                      sampleTotal += sample['size']
                      average = ( sample['sum'] / sample['size'] ).round
                    else
                      average = '0'
                    end

                    if subject != "operation"
                      benchmark = sample['metBenchmark']
                      percentage = "( #{percentage( sample['size'], benchmark )}% )"
                    end
                  end
                  "<td>#{average} #{percentage}</td>"
                }.join}
              </tr>
            "}.join }
            <tr>
              <td>All</td>
              <td>#{result['visits']['national']['visits']}</td>
              <td>#{result['visits']['national']['quota']}</td>
              #{reportSettings['fluency']['subjects'].map{ | subject |
                sample = result['visits']['national']['fluency'][subject]
                if sample.nil?
                  average = "no data"
                else
                  if sample && sample['size'] != 0 && sample['sum'] != 0
                    average = ( sample['sum'] / sample['size'] ).round
                  else
                    average = '0'
                  end

                  if subject != "operation"
                    benchmark = sample['metBenchmark']
                    percentage = "( #{percentage( sample['size'], benchmark )}% )"
                  end
                end
                "<td>#{average} #{percentage}</td>"
              }.join}
            </tr>
        </tbody>
      </table>
    "

    zoneTableHtml = "
      <label for='county-select'>County</label>
        <select id='county-select'>
          #{
            countyList.map { | countyName |
              "<option value='#{Base64.urlsafe_encode64(countyName)}' #{"selected" if countyName.downcase == currentCountyName}>#{countyName.capitalize}</option>"
            }.join("")
          }
        </select>
      <table>
        <thead>
          <tr>
            <th>Zone</th>
            <th>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a></th>
            <th>Targeted number of classroom visits<a href='#footer-note-2'><sup>[2]</sup></a></th>
            #{reportSettings['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
              "<th class='sorting'>
                #{subjectLegend[subject]}<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
          </tr>
        </thead>
        <tbody>
          #{result['visits']['byCounty'][currentCountyName]['zones'].map{ | zoneName, zone |

            row += 1

            zoneName = zoneName.downcase
            visits = zone['visits']
            quota = zone['quota']
            met = zone['fluency']['metBenchmark']
            sampleTotal = 0
            
            # Do we still need this?
            #nonFormalAsterisk = if formalZones[zone.downcase] then "<b>*</b>" else "" end

          "
            <tr> 
              <td>#{zoneName.capitalize}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{reportSettings['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
                sample = zone['fluency'][subject]
                if sample.nil?
                  average = "no data"
                else
                  
                  if sample && sample['size'] != 0 && sample['sum'] != 0
                    sampleTotal += sample['size']
                    average = ( sample['sum'] / sample['size'] ).round
                  else
                    average = '0'
                  end

                  if subject != 'operation'
                    benchmark = sample['metBenchmark']
                    percentage = "( #{percentage( sample['size'], benchmark )}% )"
                  end

                end

                "<td>#{average} #{percentage}</td>"
              }.join}

            </tr>
          "}.join }
        </tbody>
      </table>
      <small>

      <ol>
        <li id='footer-note-1'><b>Numbers of classroom visits are</b> defined as TUSOME classroom observations that include all forms and all 3 pupils assessments, with at least 20 minutes duration, and took place between 7AM and 3.10PM of any calendar day during the selected month.</li>
        <li id='footer-note-2'><b>Targeted number of classroom visits</b> is equivalent to the number of class 1 teachers in each zone.</li>
        <li id='footer-note-3'><b>Correct per minute</b> is the calculated average out of all individual assessment results from all qualifying classroom visits in the selected month to date, divided by the total number of assessments conducted.</li>
        <li id='footer-note-4'><b>Percentage at KNEC benchmark</b> is the percentage of those students that have met the KNEC benchmark for either Kiswahili or English, and for either class 1 or class 2, out of all of the students assessed for those subjects.</li>
      </ol>
      </small>

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
        </style>

        <link rel='stylesheet' type='text/css' href='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/css/jquery.dataTables.css'>
        <link rel='stylesheet' type='text/css' href='http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.css'>
        <link rel='stylesheet' type='text/css' href='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/MarkerCluster.css'>
        <link rel='stylesheet' type='text/css' href='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/MarkerCluster.Default.css'>
        

        <script src='http://cdnjs.cloudflare.com/ajax/libs/moment.js/2.9.0/moment.min.js'></script>

        <script src='/javascript/base64.js'></script>
        <script src='http://code.jquery.com/jquery-1.11.0.min.js'></script>
        <script src='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/jquery.dataTables.min.js'></script>
        <script src='http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.js'></script>
        <script src='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/leaflet.markercluster.js'></script>
        <script src='/javascript/leaflet/leaflet-providers.js'></script>
        <script src='/javascript/leaflet/leaflet.ajax.min.js'></script>

        <script src='http://d3js.org/d3.v3.min.js'></script>
        <script src='http://dimplejs.org/dist/dimple.v2.0.0.min.js'></script>
        <script src='http://d3js.org/queue.v1.min.js'></script>

        <script>

          #{chartJs}

          updateMap = function() {

            if ( window.markers == null || window.map == null || window.geoJsonLayer == null ) { return; }

            window.markers.addLayer(window.geoJsonLayer);
            window.map.addLayer(window.markers);
            $('#map-loading').remove();

          };

          $(document).ready( function() {

            initChart()

            $('table').dataTable( { iDisplayLength :-1, sDom : 't'});

            $('select').on('change',function() {
              year    = $('#year-select').val().toLowerCase()
              month   = $('#month-select').val().toLowerCase()
              county  = $('#county-select').val();

              document.location = 'http://#{$settings[:host]}#{$settings[:basePath]}/report/#{group}/#{workflowIds}/'+year+'/'+month+'/'+county+'.html';
            });

            var
              layerControl,
              osm
            ;


            L.Icon.Default.imagePath = 'http://ntp.tangerinecentral.org/images/leaflet'

            window.map = new L.Map('map');

            osm = new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              minZoom: 1,
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
            
            // ready map data

            //var geojson = {
            //  'type'     : 'FeatureCollection',
            //  'features' : {} //{#geojson.to_json}
            //};

            window.geoJsonLayer = new L.GeoJSON.AJAX(base+'reportData/#{group}/report-aggregate-geo-year#{year.to_i}month#{month.to_i}-#{Base64.urlsafe_encode64(currentCountyName.downcase)}.geojson', {
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
        <h1><img style='vertical-align:middle;' src=\"#{$settings[:basePath]}/images/corner_logo.png\" title=\"Go to main screen.\"> Kenya National Tablet Programme</h1>
  
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

        <h2>Counties</h2>
        #{countyTableHtml}
        <br>
        <div id='charts'>
          <span id='charts-loading'>Loading charts...</span>
        </div>

        <br>

        <h2>
          #{county.capitalize} County Report
          #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
        </h2>
        #{zoneTableHtml}
        
        
        <div id='map-loading'>Please wait. Data loading...</div>
        <div id='map' style='height: 400px'></div>
        
        </body>
      </html>
      "

    
    return html


  end # of report

end
