class Brockman < Sinatra::Base

  #
  # Start of report
  #

  get '/report/:group/:workflowIds/:year/:month/:county.:format?' do | group, workflowIds, year, month, county, format |

    format = "html" unless format == "json"

    unless format == "json"
      $settings[:host] = "localhost:5984"
      url = "http://#{$settings[:login]}@#{$settings[:host]}/#{group}/_design/#{$settings[:designDoc]}/_list/geojson/spirtRotut?group=true&county=#{county}&startTime=#{Time.new(year.to_i,month.to_i).to_i*1000}&endTime=#{Time.new(year.to_i,month.to_i+1).to_i*1000}&workflowIds=#{workflowIds}"
# Request was timing out - TODO optimize the list geojson
      #georesponse = RestClient.get(url)
      #puts url
      #puts Time.now


      georesponse = RestClient::Request.execute(:method => :get, :url => url, :timeout => 1000, :open_timeout => 1000)
      #puts Time.now
      features = georesponse.split("\n")
      geojson = []
      for feature in features
        geojson.push JSON.parse(feature)
      end
    end

    requestId = SecureRandom.base64

    $logger.info "email - #{group}"

    couch = Couch.new({
      :host      => $settings[:host],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    #
    # Get the uploader password and pass cookie
    #

    settings = couch.getRequest({ :document => "settings" })

    username = "uploader-#{settings['groupName']}"
    password = settings['upPass']

    postResponse = couch.postRequest({
      :db => "_session",
      :json => false,
      :data => {
        "name" => username,
        "password" => password
      }
    })

    authSession = postResponse.cookies['AuthSession']

    response.set_cookie 'AuthSession',
    {
      :value   => authSession, 
      :max_age => "600",
      :domain  => "tangerinecentral.org",
      :path    => "/"
     }

    # @hardcode who is formal
    formalZones = ["waruku","posta","silanga","kayole","gichagi","congo","zimmerman","chokaa"]
    subjectLegend = { "english_word" => "English", "word" => "Kiswahili", "operation" => "Maths" }


    geography = couch.getRequest({ :document => "geography-quotas" })
    quotasByZones  = {}
    quotasByCounty = {}
    quotaNational = 0

    geography['counties'].map { | countyName, county |
      countyName = countyTranslate(countyName.downcase)
      quotasByCounty[countyName] = county['quota']
      county['zones'].map { | zone, quota |
        zone = zoneTranslate(zone)
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

    result['visits'] = CacheHandler::tryCache "email-visits-#{monthGroup}-#{tripKeys.join}", lambda {
      byZone = {}
      byCounty = {}
      national = 0

      for sum in tripRows
        next if sum['value']['zone'].nil?
        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)

        byZone[zoneName] = 0 unless byZone[zoneName]
        byZone[zoneName] += 1

        byCounty[countyName] = 0 unless byCounty[countyName]
        byCounty[countyName] += 1

        byCounty['all'] = 0 unless byCounty['all']
        byCounty['all'] += 1

        national += 1 

      end
      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }
    }


    result['fluency'] = CacheHandler::tryCache "email-fluency-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone = {}
      byCounty = {}
      national = {}
      subjects = []

      for sum in tripRows

        next if sum['value']['zone'].nil? or sum['value']['itemsPerMinute'].nil?
        next if sum['value']['subject'].nil? or sum['value']['subject'] == "" 

        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)
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


        byCounty['all']                  = {} unless byCounty['all']
        byCounty['all'][subject]         = {} unless byCounty['all'][subject]
        byCounty['all'][subject]['sum']  = 0  unless byCounty['all'][subject]['sum']
        byCounty['all'][subject]['size'] = 0  unless byCounty['all'][subject]['size']

        byCounty['all'][subject]['sum']  += total
        byCounty['all'][subject]['size'] += benchmarked



        national                  = {} unless national
        national[subject]         = {} unless national[subject]
        national[subject]['sum']  = 0  unless national[subject]['sum']
        national[subject]['size'] = 0  unless national[subject]['size']

        national[subject]['sum']  += total
        national[subject]['size'] += benchmarked

      end

      subjects = subjects.select { |x| subjectLegend.keys.include? x }
      subjects = subjects.sort_by { |x| subjectLegend.keys.index(x) }

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national,
        "subjects" => subjects
      }
    } # result['fluency']

    result['metBenchmark'] = CacheHandler::tryCache "email-met-benchmark-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone   = {}
      byCounty = {}
      national = {}

      for sum in tripRows

        next if sum['value']['zone'].nil? or sum['value']['subject'].nil?

        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)
        subject    = sum['value']['subject'].downcase

        met = sum['value']['metBenchmark']

        byZone[zoneName] = {} unless byZone[zoneName]
        byZone[zoneName][subject] = 0 unless byZone[zoneName][subject]
        byZone[zoneName][subject] += met

        byCounty[countyName] = {} unless byCounty[countyName]
        byCounty[countyName][subject] = 0 unless byCounty[countyName][subject]
        byCounty[countyName][subject] += met

        byCounty['all'] = {} unless byCounty['all']
        byCounty['all'][subject] = 0 unless byCounty['all'][subject]
        byCounty['all'][subject] += met

        national[subject] = 0 unless national[subject]
        national[subject] += met

      end

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }

    } # result['metBenchmark']



    result['zonesByCounty'] = CacheHandler::tryCache "email-zones-by-county-#{monthGroup}-#{tripKeys.join}", lambda {
      counties = {}
      for sum in tripRows

        next if sum['value']['zone'].nil?

        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)

        counties[countyName] = [] unless counties[countyName]
        counties[countyName].push(zoneName) unless counties[countyName].include?(zoneName)

        counties['all'] = [] unless counties['all']
        counties['all'].push(zoneName) unless counties['all'].include?(zoneName)
      end
      return counties
    }

    if county.downcase == "all"
      zones = result['visits']['byZone'].keys
    elsif ! result['zonesByCounty'][county.downcase].nil?
      zones = result['zonesByCounty'][county.downcase].sort_by{|word| word.downcase}
    else
      zones = []
    end

    return result.to_json if format == "json"


    #
    # Output
    #



    #
    # Awesome chart stuff
    #


    chartJs = "

      // called on document ready
      var initChart = function()
      {
        var TREND_MONTHS = 3;  // number of months to try to pull into trend
        var month        = 5;  // starting month
        var year         = 2014; // starting year
      
        var base = '/'; // will need to update this for live development
        var quotas_link = '/#{group}/geography-quotas';

        // create links for trends by month
        for ( var i = TREND_MONTHS; i > 0; i-- )
        {
          dates[i] = { month:month--, year:year};
          if(month==0)
          {
            year--;
            month = 12;
          }
          dates[i].link = base+'_csv/report/group-tutor_feb_25/00b0a09a-2a9f-baca-2acb-c6264d4247cb,c835fc38-de99-d064-59d3-e772ccefcf7d/'+dates[i].year+'/'+dates[i].month+'/nairobi.json';
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
        
        var quota = #{geography.to_json};

        // loop over data and build d3 friendly dataset 
        dates.forEach(function(el){
          var tmpset = Array();
          for(var county in el.data.fluency.byCounty)
          {
            if (county == 'all') { continue; }
            var tmp = Object();
            tmp.County = capitalize(county);
            tmp.MonthInt = el.month;
            tmp.Year = el.year;
            tmp.Month = months[el.month];
            
            tmp['English Score'] = safeRead(el.data.fluency.byCounty[county],'english_word','sum')/safeRead(el.data.fluency.byCounty[county],'english_word','size');
            if(isNaN(tmp['English Score'])) delete tmp['English Score'];

            tmp['Kiswahili Score'] = safeRead(el.data.fluency.byCounty[county],'word','sum')/safeRead(el.data.fluency.byCounty[county],'word','size');
            if(isNaN(tmp['Kiswahili Score'])) delete tmp['Kiswahili Score'];

            tmp['Math Score'] = safeRead(el.data.fluency.byCounty[county],'operation','sum')/safeRead(el.data.fluency.byCounty[county],'operation','size');
            if(isNaN(tmp['Math Score'])) delete tmp['Math Score'];

            var countyVisits = safeRead(el.data.visits.byCounty,county);
            var countyQuota = safeRead(quota.counties,capitalize(county),'quota');
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
        addChart('Math Score', 'Maths Score', 'Correct Items Per Minute');
        addChart('Visit Attainment', 'TAC Tutor Classroom Observations','Percentage');
        //console.log(dataset); 
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
        chartObject.width = 500;
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
          chart.addLegend(chartObject.width-75, chartObject.height/2-25, 150,  100, 'left');
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

    #
    #
    #


    row = 0
    countyTableHtml = "
      <table>
        <thead>
          <tr>
            <th>County</th>
            <th>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a></th>
            <th>Targeted number of classroom visits<a href='#footer-note-2'><sup>[2]</sup></a></th>
            #{result['fluency']['subjects'].map{ | subject |
              "<th>#{subjectLegend[subject]}<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
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
              <td>#{county.capitalize}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].map{ | subject |
                sample = result['fluency']['byCounty'][county][subject]
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
                    benchmark = result['metBenchmark']['byCounty'][county][subject]
                    percentage = "( #{percentage( sample['size'], benchmark )}% )"
                  end
                end
                "<td>#{average} #{percentage}</td>"
              }.join}
            </tr>
          "}.join }
        </tbody>
      </table>
    "

    zoneTableHtml = "
      <label for='county-select'>Select county</label>
      <select id='county-select'>
        <option value='all'>All</option>
        #{
          result['zonesByCounty'].map { |county, zones|
            "<option #{"selected" if county == params[:county]}>#{county}</option>"
          }.join("")
        }
      </select>
      <table>
        <thead>
          <tr>
            <th>Zone</th>
            <th>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a></th>
            <th>Targeted number of classroom visits<a href='#footer-note-2'><sup>[2]</sup></a></th>
            #{result['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
              "<th class='sorting'>
                #{subjectLegend[subject]}<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
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

            nonFormalAsterisk = if formalZones.include? zone.downcase then "<b>*</b>" else "" end

          "
            <tr> 
              <td>#{zone.capitalize} #{nonFormalAsterisk}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
                sample = result['fluency']['byZone'][zone][subject]
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
                    benchmark = result['metBenchmark']['byZone'][zone][subject]
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
        <li id='footer-note-1'><b>Number of classroom visits</b> are defined as Full PRIMR or Best Practices classroom observations that include all forms and all 3 assessments, with at least 20 minutes duration, and took place between 7AM and 2PM of any calendar day during the selected month.</li>
        <li id='footer-note-2'><b>Targeted number of classroom visits</b> is equivalent to the number of class 1 and class 2 teachers in each zone.</li>
        <li id='footer-note-3'><b>Correct per minute</b> is the calculated average out of all individual assessment results from all qualifying classroom visits in the selected month to date, divided by the total number of assessments conducted.</li>
        <li id='footer-note-4'><b>Percentage at KNEC benchmark</b> is the percentage of those students that have met the KNEC benchmark for either Kiswahili or English, and for either class 1 or class 2, out of all of the students assessed for those subjects.</li>
      </ol>
      <ul style='list-style:none;'>
        <li><b>*</b> Non-formal</li>
      </ul>
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

        <script src='http://code.jquery.com/jquery-1.11.0.min.js'></script>
        <script src='http://ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/jquery.dataTables.min.js'></script>
        <script src='http://cdn.leafletjs.com/leaflet-0.7.2/leaflet.js'></script>
        <script src='http://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/0.4.0/leaflet.markercluster.js'></script>
        <script src='http://198.211.116.23/javascript/leaflet/leaflet-providers.js'></script>

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
              county  = $('#county-select').val().toLowerCase()

              document.location = 'http://#{$settings[:host]}/_csv/report/#{group}/#{workflowIds}/'+year+'/'+month+'/'+county+'.html';
            });

            var
              layerControl,
              osm
            ;


            L.Icon.Default.imagePath = 'http://198.211.116.23/images/leaflet'

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

            var features = #{geojson.to_json};

            var geojson = {
              'type'     : 'FeatureCollection',
              'features' : features
            };

            window.geoJsonLayer = L.geoJson( geojson, {
              onEachFeature: function( feature, layer ) {
                var html = '';

                if (feature != null && feature.properties != null && feature.properties.length != null )
                {
                  feature.properties.forEach(function(cell){
                    html += '<b>' + cell.label + '</b> ' + cell.value + '<br>';
                  });
                }
                
                layer.bindPopup( html );
              } // onEachFeature
            }); // geoJson

            window.updateMap();   

          });

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