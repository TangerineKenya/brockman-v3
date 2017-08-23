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
    
    countyId = county

    requestId = SecureRandom.base64

    TRIP_KEY_CHUNK_SIZE = 500

    couch = Couch.new({
      :host      => $settings[:dbHost],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    subjectLegend = { "english_word" => "English", "word" => "Kiswahili", "operation"=>"Maths" } 

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

    
    currentCountyId       = nil
    currentCounty         = nil
    currentCountyName     = nil

   
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

    #retrieve a county list for the select and sort it

    chartJs = "
      function titleize(str){
        return str.replace(/\\w\\S*/g, function(txt) {
          return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
        }).replace(/apbet/gi, 'APBET');
      }

      var base = 'http://#{$settings[:host]}#{$settings[:basePath]}/'; // will need to update this for live development

      // called on document ready
      var initChart = function()
      {
        var TREND_MONTHS = 3;  // number of months to try to pull into trend
        var month        = #{month.to_i};  // starting month
        var year         = #{year.to_i}; // starting year
        var countyId  = '#{countyId}';

        var reportMonth = moment(new Date(year, month, 1));
      
        var quotas_link = '/#{group}/geography-quotas';



        dates[TREND_MONTHS]       = { month:month, year:year};
        dates[TREND_MONTHS].link  = base+'reportData/#{group}/report-aggregate-year#{year.to_i}month#{month.to_i}.json';
        
        var skipMonths = [-1,0,4,8,11,12];
        var skippedMonths = 0;
        // create links for trends by month
        for ( var i = TREND_MONTHS-1; i > 0; i-- ) {
          tgtMonth      = reportMonth.clone().subtract((TREND_MONTHS - i + 1 + skippedMonths), 'months');
          if(skipMonths.indexOf(tgtMonth.get('month')+1) != -1){
            tgtMonth = tgtMonth.subtract(++skippedMonths, 'months');
          }

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



      var datasetScores = Array()
      var datasetObservationsPublic = Array();
      var datasetObservationsAPBET = Array();
      //maths
      var datasetMathsObservationsPublic = Array();
      var datasetMathsObservationsAPBET = Array();
      var datasetMathsObservationsZone =  Array();

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
            var tmpCounty = titleize(safeRead(el.data.visits.byCounty[county], 'name'));
            var tmp = {
              County   : tmpCounty,
              MonthInt : el.month,
              Year     : el.year,
              Month    : months[el.month]
            };
            
            var tmpVisit = {};
            var countyVisits = safeRead(el.data.visits.byCounty[county], 'visits');
            var countyQuota = safeRead(el.data.visits.byCounty[county],'quota');
            if (countyVisits == 0 || countyQuota == 0){
              tmpVisit['Visit Attainment'] = 0;
            } else {
              tmpVisit['Visit Attainment'] = countyVisits / countyQuota * 100;
            }

            if(tmpCounty.search(/apbet/i) == -1){
              datasetObservationsPublic.push($.extend({}, tmp, tmpVisit));
            } else {
              datasetObservationsAPBET.push($.extend({}, tmp, tmpVisit));
            }

            if(isNaN(tmpVisit['Visit Attainment'])) delete tmpVisit['Visit Attainment'];
            
            tmp['English Score - Class 1'] = safeRead(el.data.visits.byCounty[county].fluency.class[1],'english_word','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[1],'english_word','size');
            tmp['English Score - Class 2'] = safeRead(el.data.visits.byCounty[county].fluency.class[2],'english_word','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[2],'english_word','size');
            tmp['English Score - Class 3'] = safeRead(el.data.visits.byCounty[county].fluency.class[3],'english_word','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[3],'english_word','size');
            if(isNaN(tmp['English Score - Class 1'])) { delete tmp['English Score - Class 1'] };
            if(isNaN(tmp['English Score - Class 2'])) { delete tmp['English Score - Class 2'] };
            if(isNaN(tmp['English Score - Class 3'])) { delete tmp['English Score - Class 3'] };
            
            tmp['Kiswahili Score - Class 1'] = safeRead(el.data.visits.byCounty[county].fluency.class[1],'word','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[1],'word','size');
            tmp['Kiswahili Score - Class 2'] = safeRead(el.data.visits.byCounty[county].fluency.class[2],'word','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[2],'word','size');
            tmp['Kiswahili Score - Class 3'] = safeRead(el.data.visits.byCounty[county].fluency.class[3],'word','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[3],'word','size');
            if(isNaN(tmp['Kiswahili Score - Class 1'])) { delete tmp['Kiswahili Score - Class 1'] };
            if(isNaN(tmp['Kiswahili Score - Class 2'])) { delete tmp['Kiswahili Score - Class 2'] };
            if(isNaN(tmp['Kiswahili Score - Class 3'])) { delete tmp['Kiswahili Score - Class 3'] };

            //tmp['Math Score'] = safeRead(el.data.visits.byCounty[county].fluency,'operation','sum')/safeRead(el.data.visits.byCounty[county].fluency,'operation','size');
            //if(isNaN(tmp['Math Score'])) { delete tmp['Math Score'] };

            tmp['Maths Score - Class 1'] = safeRead(el.data.visits.byCounty[county].fluency.class[1],'operation','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[1],'operation','size');
            tmp['Maths Score - Class 2'] = safeRead(el.data.visits.byCounty[county].fluency.class[2],'operation','sum')/safeRead(el.data.visits.byCounty[county].fluency.class[2],'operation','size');
            if(isNaN(tmp['Maths Score - Class 1'])) { delete tmp['Maths Score - Class 1'] };
            if(isNaN(tmp['Maths Score - Class 2'])) { delete tmp['Maths Score - Class 2'] };
                          
            datasetScores.push(tmp);
          }

          //maths data
          for(var county in el.data.visits.maths.byCounty)
          {
            var tmpCounty = titleize(safeRead(el.data.visits.maths.byCounty[county], 'name'));
            var tmp = {
              County   : tmpCounty,
              MonthInt : el.month,
              Year     : el.year,
              Month    : months[el.month]
            };
            
            var tmpVisit = {};
            var countyVisits = safeRead(el.data.visits.maths.byCounty[county], 'visits');
            var countyQuota = safeRead(el.data.visits.maths.byCounty[county],'quota');
            if (countyVisits == 0 || countyQuota == 0){
              tmpVisit['Visit Attainment'] = 0;
            } else {
              tmpVisit['Visit Attainment'] = countyVisits / countyQuota * 100;
            }

            if(tmpCounty.search(/apbet/i) == -1){
              datasetMathsObservationsPublic.push($.extend({}, tmp, tmpVisit));
            } else {
              datasetMathsObservationsAPBET.push($.extend({}, tmp, tmpVisit));
            }

            if(isNaN(tmpVisit['Visit Attainment'])) delete tmpVisit['Visit Attainment'];
            
            tmp['Maths Score - Class 1'] = safeRead(el.data.visits.maths.byCounty[county].fluency.class[1],'operation','sum')/safeRead(el.data.visits.maths.byCounty[county].fluency.class[1],'operation','size');
            tmp['Maths Score - Class 2'] = safeRead(el.data.visits.maths.byCounty[county].fluency.class[2],'operation','sum')/safeRead(el.data.visits.maths.byCounty[county].fluency.class[2],'operation','size');
            if(isNaN(tmp['Maths Score - Class 1'])) { delete tmp['Maths Score - Class 1'] };
            if(isNaN(tmp['Maths Score - Class 2'])) { delete tmp['Maths Score - Class 2'] };
                          
            datasetScores.push(tmp);
          }

          //zone data
            var countyId  = '#{countyId}';
            for(var zone in el.data.visits.maths.byCounty[countyId].zones)
            {
              var tmpZone = titleize(safeRead(el.data.visits.maths.byCounty[countyId].zones[zone], 'name'));
              var tmpZoneData = {
                Zone   : tmpZone,
                MonthInt : el.month,
                Year     : el.year,
                Month    : months[el.month]
              };

              var tmpZoneVisit = {};
              var zoneVisits = safeRead(el.data.visits.maths.byCounty[countyId].zones[zone], 'visits');
              var zoneQuota = safeRead(el.data.visits.maths.byCounty[countyId].zones[zone],'quota');

              if (zoneVisits == 0 || zoneQuota == 0){
              tmpZoneVisit['Visit Attainment'] = 0;
              } else {
                tmpZoneVisit['Visit Attainment'] =  zoneVisits / zoneQuota * 100;
              }
              
              datasetMathsObservationsZone.push($.extend({}, tmpZoneData, tmpZoneVisit));
            }
        })
        
        // Build the charts. 
        addChart(datasetScores, 'English Score - Class 1', 'English Score - Class 1', 'Correct Items Per Minute');
        addChart(datasetScores, 'English Score - Class 2', 'English Score - Class 2', 'Correct Items Per Minute');
        addChart(datasetScores, 'English Score - Class 3', 'English Score - Class 3', 'Correct Items Per Minute');
        addChart(datasetScores, 'Kiswahili Score - Class 1', 'Kiswahili Score - Class 1', 'Correct Items Per Minute');
        addChart(datasetScores, 'Kiswahili Score - Class 2', 'Kiswahili Score - Class 2', 'Correct Items Per Minute');
        addChart(datasetScores, 'Kiswahili Score - Class 3', 'Kiswahili Score - Class 3', 'Correct Items Per Minute');
        addMathsChart(datasetScores, 'Maths Score - Class 1', 'Maths Score - Class 1', 'Correct Items Per Minute');
        addMathsChart(datasetScores, 'Maths Score - Class 2', 'Maths Score - Class 2', 'Correct Items Per Minute');
        //addChart('Math Score', 'Maths Score', 'Correct Items Per Minute');
        addChart(datasetObservationsPublic, 'Visit Attainment', 'Classroom Observations (Public)','Percentage');
        addChart(datasetObservationsAPBET, 'Visit Attainment', 'Classroom Observations (APBET)','Percentage');
        //maths
        addMathsChart(datasetMathsObservationsPublic, 'Visit Attainment', 'Classroom Observations (County)','Percentage');
        addMathsZoneChart(datasetMathsObservationsZone, 'Visit Attainment', 'Classroom Observations (Zone)','Percentage');
        $('#charts-loading').remove()
        $('#maths-charts-loading').remove()
      }     

    
      function addChart(dataset, variable, title, xaxis)
      {
        // create the element that the chart lives in
        var domid = (new Date()).getTime();
        $('#charts').append('<div class=\"chart\"><h2 style=\"text-align:center;\">'+title+'</h2><div id=\"chartContainer'+domid+'\" /></div>');

        // start building chart object to pass to render function
        chartObject = new Object();
        chartObject.container = '#chartContainer'+domid;
        chartObject.height = 650;
        chartObject.width = 450;
        chartObject.data =  dataset;
        
        chartObject.plot = function(chart){

          // setup x, y and series
          var y = chart.addCategoryAxis('y', ['County','Month']);
          y.addOrderRule('County');
          y.addGroupOrderRule('MonthInt');

          var x = chart.addMeasureAxis('x', variable);

          var series = chart.addSeries(['Month'], dimple.plot.bar);
          series.addOrderRule('MonthInt');
          series.clusterBarGap = 0;
          
          // add the legend
          //chart.addLegend(chartObject.width-100, chartObject.height/2-25, 100,  150, 'left');
          chart.addLegend(60, 10, 400, 20, 'right');
        };
        
        // titles for x and y axis
        chartObject.yAxis = 'County';
        chartObject.xAxis = xaxis;
        
        // show hover tooltips
        chartObject.showHover = true;
        buildChart(chartObject);
      }
      
      function addMathsChart(dataset, variable, title, xaxis)
      {
        // create the element that the chart lives in
        var domid = (new Date()).getTime();
        $('#maths-charts').append('<div class=\"chart\"><h2 style=\"text-align:center;\">'+title+'</h2><div id=\"chartContainer'+domid+'\" /></div>');

        // start building chart object to pass to render function
        chartObject = new Object();
        chartObject.container = '#chartContainer'+domid;
        chartObject.height = 650;
        chartObject.width = 450;
        chartObject.data =  dataset;
        
        chartObject.plot = function(chart){

          // setup x, y and series
          var y = chart.addCategoryAxis('y', ['County','Month']);
          y.addOrderRule('County');
          y.addGroupOrderRule('MonthInt');

          var x = chart.addMeasureAxis('x', variable);

          var series = chart.addSeries(['Month'], dimple.plot.bar);
          series.addOrderRule('MonthInt');
          series.clusterBarGap = 0;
          
          // add the legend
          //chart.addLegend(chartObject.width-100, chartObject.height/2-25, 100,  150, 'left');
          chart.addLegend(60, 10, 400, 20, 'right');
        };
        
        // titles for x and y axis
        chartObject.yAxis = 'County';
        chartObject.xAxis = xaxis;
        
        // show hover tooltips
        chartObject.showHover = true;
        buildChart(chartObject);
      }

      function addMathsZoneChart(dataset, variable, title, xaxis)
      {
        // create the element that the chart lives in
        var domid = (new Date()).getTime();
        $('#maths-charts').append('<div class=\"chart\"><h2 style=\"text-align:center;\">'+title+'</h2><div id=\"chartContainer'+domid+'\" /></div>');

        // start building chart object to pass to render function
        chartObject = new Object();
        chartObject.container = '#chartContainer'+domid;
        chartObject.height = 650;
        chartObject.width = 450;
        chartObject.data =  dataset;
        
        chartObject.plot = function(chart){

          // setup x, y and series
          var y = chart.addCategoryAxis('y', ['Zone','Month']);
          y.addOrderRule('Zone');
          y.addGroupOrderRule('MonthInt');

          var x = chart.addMeasureAxis('x', variable);

          var series = chart.addSeries(['Month'], dimple.plot.bar);
          series.addOrderRule('MonthInt');
          series.clusterBarGap = 0;
          
          // add the legend
          //chart.addLegend(chartObject.width-100, chartObject.height/2-25, 100,  150, 'left');
          chart.addLegend(60, 10, 400, 20, 'right');
        };
        
        // titles for x and y axis
        chartObject.yAxis = 'Zone';
        chartObject.xAxis = xaxis;
        
        // show hover tooltips
        chartObject.showHover = true;
        buildChart(chartObject);
      }

      function buildChart(chart)
      {
        var svg = dimple.newSvg(chart.container, chart.width, chart.height);

        //set white background for svg - helps with conversion to png
        //svg.append('rect').attr('x', 0).attr('y', 0).attr('width', chart.width).attr('height', chart.height).attr('fill', 'white');
          
        var dimpleChart = new dimple.chart(svg, chart.data);
        dimpleChart.setBounds(90, 30, chart.width-100, chart.height-100);
        chartObject.plot(dimpleChart);

        if(!chart.showHover)
        {
          dimpleChart.series[0].addEventHandler('mouseover', function(){});
          dimpleChart.series[0].addEventHandler('mouseout', function(){});
        }

        dimpleChart.draw();
        
        // x axis title and redraw bottom line after removing tick marks
        dimpleChart.axes[1].titleShape.text(chartObject.xAxis).style({'font-size':'11px', 'stroke': '#555555', 'stroke-width':'0.2px'});
        dimpleChart.axes[1].shapes.selectAll('line').remove();
        dimpleChart.axes[1].shapes.selectAll('path').attr('d','M90,1V0H'+String(chart.width-10)+'V1').style('stroke','#555555');
        if(!dimpleChart.axes[0].hidden)
        {
          // update y axis
          dimpleChart.axes[0].titleShape.text(chartObject.yAxis).style({'font-size':'11px', 'stroke': '#555555', 'stroke-width':'0.2px'});
          //dimpleChart.axes[0].gridlineShapes.selectAll('line').remove();
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

    #****************************** TAC Tutor Report Components *************************
    row = 0
    tutorCountyTableHtml = "
      <table class='tacTutor-table'>
        <thead>
          <tr>
            <th>County</th>
            <th class='custSort'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>

            #{reportSettings['fluency']['subjects'].map{ | subject |
              "<th class='custSort'>#{subjectLegend[subject]} - Class 1<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>
              <th class='custSort'>#{subjectLegend[subject]} - Class 2<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>
              <th class='custSort'>#{subjectLegend[subject]} - Class 3<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | countyId, county |

            countyName      = county['name']
            visits          = county['visits']
            quota           = county['quota']
            cl1sampleTotal = 0
            cl2sampleTotal = 0
            cl3sampleTotal = 0

            "
              <tr>
                <td>#{titleize(countyName)}</td>
                <td>#{visits} ( #{percentage( quota, visits )}% )</td>
                #{reportSettings['fluency']['subjects'].map{ | subject |
                  #ensure that there, at minimum, a fluency category for the county
                  
                  puts county['fluency']
                  puts countyId
                  puts county['fluency']['class']
                  puts county['fluency']['class']['1']
                  puts county['fluency']['class']['1'][subject]
                  cl1sample = county['fluency']['class']['1'][subject]
                  if cl1sample.nil?
                    cl1average = "no data"
                  else
                    if cl1sample && cl1sample['size'] != 0 && cl1sample['sum'] != 0
                      cl1sampleTotal += cl1sample['size']
                      cl1average = ( cl1sample['sum'] / cl1sample['size'] ).round
                    else
                      cl1average = '0'
                    end

                    if subject != "operation" && cl1sample['size'] != 0 && cl1sample['sum'] != 0 && cl1average !=0
                      cl1benchmark = cl1sample['metBenchmark']
                      cl1percentage = "( #{percentage( cl1sample['size'], cl1benchmark )}% )"
                    end
                  end

                  cl2sample = county['fluency']['class']['2'][subject]
                  if cl2sample.nil?
                    cl2average = "no data"
                  else
                    if cl2sample && cl2sample['size'] != 0 && cl2sample['sum'] != 0
                      cl2sampleTotal += cl2sample['size']
                      cl2average = ( cl2sample['sum'] / cl2sample['size'] ).round
                    else
                      cl2average = '0'
                    end

                    if subject != "operation" && cl2sample['size'] != 0 && cl2sample['sum'] != 0 && cl2average !=0
                      cl2benchmark = cl2sample['metBenchmark']
                      cl2percentage = "( #{percentage( cl2sample['size'], cl2benchmark )}% )"
                    end
                  end

                  cl3sample = county['fluency']['class']['3'][subject]
                  if cl3sample.nil?
                    cl3average = "no data"
                  else
                    if cl3sample && cl3sample['size'] != 0 && cl3sample['sum'] != 0
                      cl3sampleTotal += cl3sample['size']
                      cl3average = ( cl3sample['sum'] / cl3sample['size'] ).round
                    else
                      cl3average = '0'
                    end

                    if subject != "operation" && cl3sample['size'] != 0 && cl3sample['sum'] != 0 && cl3average !=0
                      cl3benchmark = cl3sample['metBenchmark']
                      cl3percentage = "( #{percentage( cl3sample['size'], cl3benchmark )}% )"
                    end
                  end

                  "<td>#{cl1average} <span>#{cl1percentage}</span></td>
                  <td>#{cl2average} <span>#{cl2percentage}</span></td>
                  <td>#{cl3average} <span>#{cl3percentage}</span></td>"
                }.join}
              </tr>
            "}.join }
            <tr>
              <td>All</td>
              <td>#{result['visits']['national']['visits']} ( #{percentage( result['visits']['national']['quota'], result['visits']['national']['visits'] )}% )</td>
              #{reportSettings['fluency']['subjects'].map{ | subject |
                cl1sample = result['visits']['national']['fluency']['class']['1'][subject]
                if cl1sample.nil?
                  cl1average = "no data"
                else
                  if cl1sample && cl1sample['size'] != 0 && cl1sample['sum'] != 0
                    cl1sampleTotal = cl1sample['size']
                    cl1average = ( cl1sample['sum'] / cl1sample['size'] ).round
                  else
                    cl1average = '0'
                  end

                  if subject != "operation" && cl1average != 0
                    cl1benchmark = cl1sample['metBenchmark']
                    cl1percentage = "( #{percentage( cl1sample['size'], cl1benchmark )}% )"
                  end
                end

                cl2sample = result['visits']['national']['fluency']['class']['2'][subject]
                if cl2sample.nil?
                  cl2average = "no data"
                else
                  if cl2sample && cl2sample['size'] != 0 && cl2sample['sum'] != 0
                    cl2sampleTotal = cl2sample['size']
                    cl2average = ( cl2sample['sum'] / cl2sample['size'] ).round
                  else
                    cl2average = '0'
                  end

                  if subject != "operation" && cl2average != 0
                    cl2benchmark = cl2sample['metBenchmark']
                    cl2percentage = "( #{percentage( cl2sample['size'], cl2benchmark )}% )"
                  end
                end

                cl3sample = result['visits']['national']['fluency']['class']['3'][subject]
                if cl3sample.nil?
                  cl3average = "no data"
                else
                  if cl3sample && cl3sample['size'] != 0 && cl3sample['sum'] != 0
                    cl3sampleTotal = cl3sample['size']
                    cl3average = ( cl3sample['sum'] / cl3sample['size'] ).round
                  else
                    cl3average = '0'
                  end

                  if subject != "operation"  && cl3average != 0
                    cl3benchmark = cl3sample['metBenchmark']
                    cl3percentage = "( #{percentage( cl3sample['size'], cl3benchmark )}% )"
                  end
                end
                "<td>#{cl1average}  <span>#{cl1percentage}</span></td>
                  <td>#{cl2average} <span>#{cl2percentage}</span></td>
                  <td>#{cl3average} <span>#{cl3percentage}</span></td>"
              }.join}
            </tr>
        </tbody>
      </table>
    "

    tutorZoneTableHtml = "
      <label for='tutor-county-select'>County</label>
        <select id='tutor-county-select'>
          #{
            orderedCounties = result['visits']['byCounty'].sort_by{ |countyId, county| county['name'] }
            orderedCounties.map{ | countyId, county |
              "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
            }.join("")
          }
        </select>
      <table class='tacTutor-table'>
        <thead>
          <tr>
            <th>Zone</th>
            <th class='custSort'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            #{reportSettings['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
              "<th class='custSort'>
                #{subjectLegend[subject]} - Class 1<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th><th class='custSort'>
                #{subjectLegend[subject]} - Class 2<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>
              </th><th class='custSort'>
                #{subjectLegend[subject]} - Class 3<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
          </tr>
        </thead>
        <tbody>
          #{result['visits']['byCounty'][currentCountyId]['zones'].map{ | zoneId, zone |

            row += 1

            zoneName = zone['name']
            visits = zone['visits']
            quota = zone['quota']
            met = zone['fluency']['metBenchmark']
            cl1sampleTotal = 0
            cl2sampleTotal = 0
            cl3sampleTotal = 0
            
            # Do we still need this?
            #nonFormalAsterisk = if formalZones[zone.downcase] then "<b>*</b>" else "" end

          "
            <tr> 
              <td>#{zoneName}</td>
              <td>#{visits} ( #{percentage( quota, visits )}% )</td>
              #{reportSettings['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
                
                cl1sample = zone['fluency']['class']['1'][subject]
                  if cl1sample.nil?
                    cl1average = "no data"
                  else
                    if cl1sample && cl1sample['size'] != 0 && cl1sample['sum'] != 0
                      cl1sampleTotal += cl1sample['size']
                      cl1average = ( cl1sample['sum'] / cl1sample['size'] ).round
                    else
                      cl1average = '0'
                    end

                    if subject != "operation" && cl1sample['size'] != 0 && cl1sample['sum'] != 0 && cl1average !=0
                      cl1benchmark = cl1sample['metBenchmark']
                      cl1percentage = "( #{percentage( cl1sample['size'], cl1benchmark )}% )"
                    end
                  end

                  cl2sample = zone['fluency']['class']['2'][subject]
                  if cl2sample.nil?
                    cl2average = "no data"
                  else
                    if cl2sample && cl2sample['size'] != 0 && cl2sample['sum'] != 0
                      cl2sampleTotal += cl2sample['size']
                      cl2average = ( cl2sample['sum'] / cl2sample['size'] ).round
                    else
                      cl2average = '0'
                    end

                    if subject != "operation" && cl2sample['size'] != 0 && cl2sample['sum'] != 0 && cl2average !=0
                      cl2benchmark = cl2sample['metBenchmark']
                      cl2percentage = "( #{percentage( cl2sample['size'], cl2benchmark )}% )"
                    end
                  end

                  cl3sample = zone['fluency']['class']['3'][subject]
                  if cl3sample.nil?
                    cl3average = "no data"
                  else
                    if cl3sample && cl3sample['size'] != 0 && cl3sample['sum'] != 0
                      cl3sampleTotal += cl3sample['size']
                      cl3average = ( cl3sample['sum'] / cl3sample['size'] ).round
                    else
                      cl3average = '0'
                    end

                    if subject != "operation" && cl3sample['size'] != 0 && cl3sample['sum'] != 0 && cl3average !=0
                      cl3benchmark = cl3sample['metBenchmark']
                      cl3percentage = "( #{percentage( cl3sample['size'], cl3benchmark )}% )"
                    end
                  end
                  "<td>#{cl1average}<span>#{cl1percentage}</span></td>
                  <td>#{cl2average} <span>#{cl2percentage}</span></td>
                  <td>#{cl3average} <span>#{cl3percentage}</span></td>"
              }.join}

            </tr>
          "}.join }
        </tbody>
      </table>
      <small>

      <ol>
        <li id='footer-note-1'><b>Numbers of classroom visits are</b> defined as TUSOME classroom observations that include all forms and all 3 pupils assessments, with at least 20 minutes duration, and took place between 7AM and 3.10PM of any school days during the selected month.</li>
        <li id='footer-note-2'><b>Targeted number of classroom visits</b> is equivalent to the number of schools in each zone.</li>
        <li id='footer-note-3'><b>Correct per minute</b> is the calculated average out of all individual assessment results from all qualifying classroom visits in the selected month to date, divided by the total number of assessments conducted.</li>
        <li id='footer-note-4'><b>Percentage at KNEC benchmark</b> is the percentage of those students that have met the KNEC benchmark for either Kiswahili or English, and for either class 1, class 2 or class 3, out of all of the students assessed for those subjects. The benchmarks for class 3 are yet to be defined.</li>
      </ol>
      </small>

    "

    #****************************** SCDE Report Components *************************
    row = 0
    scdeCountyTableHtml = "
      <table class='scde-table'>
        <thead>
          <tr>
            <th>County</th>
            <th class='custSort'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            
          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | countyId, county |

            countyName      = county['name']
            visits          = county['scde']['visits']
            quota           = county['scde']['quota']
            sampleTotal     = 0

            "
              <tr>
                <td>#{titleize(countyName)}</td>
                <td>#{visits} ( #{percentage( quota, visits )}% )</td>
                
              </tr>
            "}.join }
            <tr>
              <td>All</td>
              <td>#{result['visits']['scde']['national']['visits']} ( #{percentage( result['visits']['scde']['national']['quota'], result['visits']['scde']['national']['visits'] )}% )</td>
            </tr>
        </tbody>
      </table>
    "

    scdeSubCountyTableHtml = "
      <label for='scde-county-select'>County</label>
        <select id='scde-county-select'>
          #{
            orderedCounties = result['visits']['byCounty'].sort_by{ |countyId, county| county['name'] }
            orderedCounties.map{ | countyId, county |
              "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
            }.join("")
          }
        </select>
      <table class='scde-table'>
        <thead>
          <tr>
            <th>SubCounty</th>
            <th class='custSort'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
          </tr>
        </thead>
        <tbody>
          #{result['visits']['byCounty'][currentCountyId]['subCounties'].map{ | subCountyId, subCounty |

            row += 1

            subCountyName = subCounty['name']
            visits = subCounty['scde']['visits']
            quota = subCounty['scde']['quota']
            sampleTotal = 0
            
            # Do we still need this?
            #nonFormalAsterisk = if formalZones[zone.downcase] then "<b>*</b>" else "" end

          "
            <tr> 
              <td>#{subCountyName}</td>
              <td>#{visits} ( #{percentage( quota, visits )}% )</td>
            </tr>
          "}.join }
        </tbody>
      </table>

    "
    #****************************** ESQSC Report Components *************************
    row = 0
    esqacCountyTableHtml = "
      <table class='esqac-table'>
        <thead>
          <tr>
            <th>County</th>
            <th class='custSort'>Number of classroom visits - Moe <a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            <th class='custSort'>Number of classroom visits - Priede <a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            
          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | countyId, county |

            countyName      = county['name']
            visits          = county['moe']['visits']
           
            priedeVisits    = county['priede']['visits']
            quota           = county['priede']['quota']
            sampleTotal     = 0

            "
              <tr>
                <td>#{titleize(countyName)}</td>
                <td>#{visits} ( #{percentage( quota, visits )}% )</td>
               <td>#{priedeVisits} ( #{percentage( quota, priedeVisits )}% )</td>
                
              </tr>
            "}.join }
            <tr>
              <td>All</td>
              <td>#{result['visits']['moe']['national']['visits']} ( #{percentage( result['visits']['moe']['national']['quota'], result['visits']['moe']['national']['visits'] )}% )</td>
              <td>#{result['visits']['priede']['national']['visits']} ( #{percentage( result['visits']['priede']['national']['quota'], result['visits']['priede']['national']['visits'] )}% )</td>
              
            </tr>
        </tbody>
      </table>
    "
    esqacSubCountyTableHtml = "
      <label for='esqac-county-select'>County</label>
        <select id='esqac-county-select'>
          #{
            orderedCounties = result['visits']['byCounty'].sort_by{ |countyId, county| county['name'] }
            orderedCounties.map{ | countyId, county |
              "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
            }.join("")
          }
        </select>
      <table class='esqac-table'>
        <thead>
          <tr>
            <th>SubCounty</th>
            <th class='custSort'>Number of classroom visits - Moe <a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            <th class='custSort'>Number of classroom visits - Priede <a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            
          </tr>
        </thead>
        <tbody>
          #{result['visits']['byCounty'][currentCountyId]['subCounties'].map{ | subCountyId, subCounty |

            row += 1

            subCountyName = subCounty['name']
            visits = subCounty['moe']['visits']
            priedeVisits = subCounty['priede']['visits']
            
            quota = subCounty['moe']['quota']
            sampleTotal = 0
            
            # Do we still need this?
            #nonFormalAsterisk = if formalZones[zone.downcase] then "<b>*</b>" else "" end

          "
            <tr> 
              <td>#{subCountyName}</td>
              <td>#{visits} ( #{percentage( quota, visits )}% )</td>
              <td>#{priedeVisits} ( #{percentage( quota, priedeVisits )}% )</td>
             
            </tr>
          "}.join }
        </tbody>
      </table>

    "
    #***************************** Maths Report components ***************************
    row = 0

    if !result['visits']['maths']['national']['fluency']['class'].nil?

      cl1Allsample = result['visits']['maths']['national']['fluency']['class']['1']['operation']
        if cl1Allsample.nil?
                    cl1Allaverage = "no data"
        else
          if cl1Allsample && cl1Allsample['size'] != 0 && cl1Allsample['sum'] != 0
            cl1AllsampleTotal = cl1Allsample['size']
            cl1Allaverage = ( cl1Allsample['sum'] / cl1Allsample['size'] ).round
          else
            cl1Allaverage = '0'
          end
            cl1Allbenchmark = cl1Allsample['metBenchmark']
            cl1Allpercentage = "( #{percentage( cl1Allsample['size'], cl1Allbenchmark )}% )"
                  
        end

      cl2Allsample = result['visits']['maths']['national']['fluency']['class']['2']['operation']
        if cl2Allsample.nil?
                    cl2Allaverage = "no data"
        else
          if cl2Allsample && cl2Allsample['size'] != 0 && cl2Allsample['sum'] != 0
              cl2AllsampleTotal = cl2Allsample['size']
              cl2Allaverage = ( cl2Allsample['sum'] / cl2Allsample['size'] ).round
          else
            cl2Allaverage = '0'
          end
                    
          cl2Allbenchmark = cl2Allsample['metBenchmark']
          cl2Allpercentage = "( #{percentage( cl2Allsample['size'], cl2Allbenchmark )}% )"
                    
        end
    else
      cl1Allaverage = "no data"
      cl2Allaverage = "no data"
    end 
    mathsCountyTableHtml = "
      <table class='maths-table'>
        <thead>
          <tr>
            <th>County</th>
            <th class='custSort' align='left'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            <th class='custSort'>Maths - Class 1<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>"}
              </th>
              <th class='custSort'>Maths - Class 2<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>"}
              </th>
          </tr>
        </thead>
        <tbody>
        #{ result['visits']['maths']['byCounty'].map{ | countyId, county |

            countyName      = county['name']
            visits          = county['visits']
            quota           = county['quota']
            cl1sampleTotal = 0
            cl2sampleTotal = 0

            puts county['fluency']
            puts countyId
            puts county['fluency']['class']
            puts county['fluency']['class']['1']
            puts county['fluency']['class']['1']['operation']
            cl1sample = county['fluency']['class']['1']['operation']
            if cl1sample.nil?
              cl1average = "no data"
            else
              if cl1sample && cl1sample['size'] != 0 && cl1sample['sum'] != 0
                cl1sampleTotal += cl1sample['size']
                cl1average = ( cl1sample['sum'] / cl1sample['size'] ).round
              else
                cl1average = '0'
              end                   
                cl1benchmark = cl1sample['metBenchmark']
                cl1percentage = "( #{percentage( cl1sample['size'], cl1benchmark )}% )"
                    
            end

            cl2sample = county['fluency']['class']['2']['operation']
            if cl2sample.nil?
              cl2average = "no data"
            else
              if cl2sample && cl2sample['size'] != 0 && cl2sample['sum'] != 0
                cl2sampleTotal += cl2sample['size']
                cl2average = ( cl2sample['sum'] / cl2sample['size'] ).round
              else
                cl2average = '0'
              end                 
              cl2benchmark = cl2sample['metBenchmark']
              cl2percentage = "( #{percentage( cl2sample['size'], cl2benchmark )}% )"
                   
            end
            
           

            "<tr>
                <td>#{titleize(countyName)}</td>
                <td>#{visits} ( #{percentage( quota, visits )}% )</td>
                <td>#{cl1average} <span>#{cl1percentage}</span></td>
                <td>#{cl2average} <span>#{cl2percentage}</span></td>
            </tr>
            "}.join}
             <tr>
              <td>All</td>
              <td>#{result['visits']['maths']['national']['visits']} ( #{percentage( result['visits']['maths']['national']['quota'], result['visits']['maths']['national']['visits'] )}% )</td>
              <td>#{cl1Allaverage}<span>#{cl1Allpercentage}</span></td>
              <td>#{cl2Allaverage}<span>#{cl2Allpercentage}</span></td>
            </tr>
      </tbody>
      </table>
    "

    mathsZoneTableHtml = "
      <label for='maths-county-select'>County</label>
        <select id='maths-county-select'>
          #{
            orderedCounties = result['visits']['maths']['byCounty'].sort_by{ |countyId, county| county['name'] }
            orderedCounties.map{ | countyId, county |
              "<option value='#{countyId}' #{"selected" if countyId == currentCountyId}>#{titleize(county['name'])}</option>"
            }.join("")
          }
        </select>
      <table class='maths-table'>
        <thead>
          <tr>
            <th>Zone</th>
            <th class='custSort' align='left'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a><br>
            <small>( Percentage of Target Visits)</small></th>
            <th class='custSort'>
                Maths - Class 1<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>"}
              </th><th class='custSort'>
                Maths - Class 2<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>"}
              </th>
          </tr>
        </thead>
        <tbody>
          #{result['visits']['maths']['byCounty'][currentCountyId]['zones'].map{ | zoneId, zone |

            row += 1

            zoneName = zone['name']
            visits = zone['visits']
            quota = zone['quota']
            met = zone['fluency']['metBenchmark']
            cl1sampleTotal = 0
            cl2sampleTotal = 0
            
            cl1sample = zone['fluency']['class']['1']['operation']
            if cl1sample.nil?
                    cl1average = "no data"
              else
                if cl1sample && cl1sample['size'] != 0 && cl1sample['sum'] != 0
                  cl1sampleTotal += cl1sample['size']
                  cl1average = ( cl1sample['sum'] / cl1sample['size'] ).round
                else
                  cl1average = '0'
                end
                  cl1benchmark = cl1sample['metBenchmark']
                  cl1percentage = "( #{percentage( cl1sample['size'], cl1benchmark )}% )"  
            end
              cl2sample = zone['fluency']['class']['2']['operation']
              if cl2sample.nil?
                cl2average = "no data"
              else
                if cl2sample && cl2sample['size'] != 0 && cl2sample['sum'] != 0
                  cl2sampleTotal += cl2sample['size']
                  cl2average = ( cl2sample['sum'] / cl2sample['size'] ).round
                else
                  cl2average = '0'
                end
                  cl2benchmark = cl2sample['metBenchmark']
                  cl2percentage = "( #{percentage( cl2sample['size'], cl2benchmark )}% )" 
          end

            # Do we still need this?
            #nonFormalAsterisk = if formalZones[zone.downcase] then "<b>*</b>" else "" end

          "
            <tr> 
              <td>#{zoneName}</td>
              <td>#{visits} ( #{percentage( quota, visits )}% )</td>
              <td>#{cl1average} <span>#{cl1percentage}</span></td>
              <td>#{cl2average} <span>#{cl2percentage}</span></td>
              </tr>
          "}.join }
        </tbody>
      </table>
      <small>

      <ol>
        <li id='footer-note-1'><b>Numbers of classroom visits are</b> defined as TUSOME classroom observations that include all forms and all 3 pupils assessments, with at least 20 minutes duration, and took place between 7AM and 3.10PM of any calendar day during the selected month.</li>
        <li id='footer-note-2'><b>Targeted number of classroom visits</b> is equivalent to the number of class 1 teachers in each zone.</li>
        <li id='footer-note-3'><b>Correct per minute</b> is the calculated average out of all individual assessment results from all qualifying classroom visits in the selected month to date, divided by the total number of assessments conducted.</li>
        <li id='footer-note-4'><b>Percentage at KNEC benchmark</b> is the percentage of those students that have met the KNEC benchmark for either class 1 or class 2, out of all of the students assessed for those subjects. The benchmarks are yet to be defined.</li>
      </ol>
      </small>

    "

    #************************ Tab Definition ************************
    tutorTabContent = "
      
      <h2>CSO Report (#{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]})</h2>
      <hr>
      <h2>Counties</h2>
      #{tutorCountyTableHtml}
      <br>
      <div id='charts'>
        <span id='charts-loading'>Loading charts...</span>
      </div>

      <br>

      <h2>
        #{titleize(currentCountyName)} County Report
      </h2>
      #{tutorZoneTableHtml}
      
      
      <div id='tutor-map-loading'>Please wait. Data loading...</div>
      <div id='tutor-map' style='height: 400px'></div>
      <br>
      <a id='tutor-view-all-btn' class='btn' href='#'>View All County Data</a>
    "

    scdeTabContent = "
      <h2>SCDE Report (#{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]})</h2>
      <hr>
      <h2>Counties</h2>
      #{scdeCountyTableHtml}
      <br>
      <hr>
      <h2>
        #{titleize(currentCountyName)} County Report
      </h2>
      #{scdeSubCountyTableHtml}
      
      <br>
      <div id='scde-map-loading'>Please wait. Data loading...</div>
      <div id='scde-map' style='height: 400px'></div>
      <br>
      <a id='scde-view-all-btn' class='btn' href='#'>View All County Data</a>
    "

    esqacTabContent = "
      <h2>ESQAC Report (#{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]})</h2>
      <hr>
      <h2>Counties</h2>
      #{esqacCountyTableHtml}
      <br>

      <h2>
        #{titleize(currentCountyName)} County Report
        #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
      </h2>
      #{esqacSubCountyTableHtml}
      
      <br>
      <div id='esqac-map-loading'>Please wait. Data loading...</div>
      <div id='esqac-map' style='height: 400px'></div>
      <br>
      <a id='esqac-view-all-btn' class='btn' href='#'>View All County Data</a>
    "

    mathTabContent = "
      <h2>Maths Report (#{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]})</h2>
      <hr>
      <h2>Counties</h2>
      #{mathsCountyTableHtml}
      <br>
      <div id='maths-charts'>
        <span id='maths-charts-loading'>Loading charts...</span>
      </div>

      <br>

      <h2>
        #{titleize(currentCountyName)} County Report
      </h2>
      #{mathsZoneTableHtml}
      
      
      <div id='maths-map-loading'>Please wait. Data loading...</div>
      <div id='maths-map' style='height: 400px'></div>
      <br>
      <a id='maths-view-all-btn' class='btn' href='#'>View All County Data</a>
      "

    html =  "
    <html>
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
          //google analytics tracking
          (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
          (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
          m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
          })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
         
          ga('create', 'UA-103508683-2', 'auto');
          ga('send', 'pageview');

          #{chartJs}

          updateMap = function() {

            if ( window.markers == null || window.map == null || window.geoJsonLayer == null ) { return; }

            window.markers.addLayer(window.geoJsonLayer);
            window.map.addLayer(window.markers);
            $('#map-loading').hide();

          };

          var mapDataURL = new Array();
          mapDataURL['current'] = base+'reportData/#{group}/report-aggregate-geo-year#{year.to_i}month#{month.to_i}-#{currentCountyId}.geojson';
          mapDataURL['all'] = new Array();

          mapDataURL['all']
          #{
            result['visits']['byCounty'].map{ | countyId, county |
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
            } );
          
          L.Icon.Default.imagePath = 'http://ntp.tangerinecentral.org/images/leaflet'
          var pageMaps = {}
          var mapControls = {
            tutor: {
              osm: new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                minZom: 1,
                maxZoom: 12,
                attribution: 'Map data © OpenStreetMap contributors'
              }),
              layerControl: L.control.layers.provided(['OpenStreetMap.Mapnik','Stamen.Watercolor']),
              markers: L.markerClusterGroup(),
              layerGeoJsonFilter: function(feature, layer){
                return (feature.role === 'tac-tutor' || feature.role === 'coach');
              }
            },
            maths: {
              osm: new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                minZom: 1,
                maxZoom: 12,
                attribution: 'Map data © OpenStreetMap contributors'
              }),
              layerControl: L.control.layers.provided(['OpenStreetMap.Mapnik','Stamen.Watercolor']),
              markers: L.markerClusterGroup(),
              layerGeoJsonFilter: function(feature, layer){
                return (feature.role === 'maths');
              }
            },
            scde: {
              osm: new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                minZom: 1,
                maxZoom: 12,
                attribution: 'Map data © OpenStreetMap contributors'
              }),
              layerControl: L.control.layers.provided(['OpenStreetMap.Mapnik','Stamen.Watercolor']),
              markers: L.markerClusterGroup(),
              layerGeoJsonFilter: function(feature, layer){
                return (feature.role === 'scde');
              }
            },
            esqac: {
              osm: new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                minZom: 1,
                maxZoom: 12,
                attribution: 'Map data © OpenStreetMap contributors'
              }),
              layerControl: L.control.layers.provided(['OpenStreetMap.Mapnik','Stamen.Watercolor']),
              markers: L.markerClusterGroup(),
              layerGeoJsonFilter: function(feature, layer){
                return (feature.role === 'esqac');
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
            //if there is a hash in the URL, change the tab to match it
            var hash = location.hash.replace(/^#/, '');
            forceTabSelect(hash);

            initChart()
            
            /***********
            **
            **   Init Custom Data Tables
            **
            ************/
            //init display for the TAC Tutor Tab
            $('table.tacTutor-table').dataTable( { 
              iDisplayLength :-1, 
              sDom : 't',
              aoColumnDefs: [
                 { sType: 'num-html', aTargets: [1,2,3,4,5] }
               ]
            });

            //init display for the SCDE Tab
            $('table.scde-table').dataTable( { 
              iDisplayLength :-1, 
              sDom : 't'
            });

            //init display for the SCDE Tab
            $('table.esqac-table').dataTable( { 
              iDisplayLength :-1, 
              sDom : 't'
            });

            //init display for the Maths Tab
            $('table.maths-table').dataTable( { 
              iDisplayLength :-1, 
              sDom : 't'
            });
              
            /***********
            **
            **   Init Select Handlers
            **
            ************/
            var currCounty = '#{countyId}';
            $('#year-select,#month-select').on('change',function() {
              reloadReport();
            });

            $('#tutor-county-select').on('change',function() {
              currCounty = $('#tutor-county-select').val()
              reloadReport();
            });

            $('#scde-county-select').on('change',function() {
              currCounty = $('#scde-county-select').val()
              reloadReport();
            });

            $('#esqac-county-select').on('change',function() {
              currCounty = $('#esqac-county-select').val()
              reloadReport();
            });

            $('#maths-county-select').on('change',function() {
              currCounty = $('#maths-county-select').val()
              reloadReport();
            });

            function reloadReport(){
              year    = $('#year-select').val().toLowerCase()
              month   = $('#month-select').val().toLowerCase()

              document.location = 'http://#{$settings[:host]}#{$settings[:basePath]}/report/#{group}/#{workflowIds}/'+year+'/'+month+'/'+currCounty+'.html'+location.hash;
            }

            
            /***********
            **
            **   Init Leaflet Maps
            **
            ************/

            
            window.markers = L.markerClusterGroup();
            
            pageMaps.tutor = new L.Map('tutor-map');
            pageMaps.scde  = new L.Map('scde-map');
            pageMaps.esqac = new L.Map('esqac-map');
            pageMaps.maths = new L.Map('maths-map');
            
            //----------- TUTOR MAP CONFIG -------------------------
            pageMaps.tutor.addLayer(mapControls.tutor.osm);
            pageMaps.tutor.setView(new L.LatLng(0, 35), 6);
            mapControls.tutor.layerControl.addTo(pageMaps.tutor);
            mapControls.tutor.geoJsonLayer = new L.GeoJSON.AJAX(mapDataURL['current'], {
              onEachFeature: layerOnEachFeature,
              filter: mapControls.tutor.layerGeoJsonFilter
            });
            mapControls.tutor.geoJsonLayer.on('data:loaded', function(){
              if ( mapControls.tutor.markers == null || pageMaps.tutor == null || mapControls.tutor.geoJsonLayer == null ) { return; }
              mapControls.tutor.markers.addLayer(mapControls.tutor.geoJsonLayer);
              pageMaps.tutor.addLayer(mapControls.tutor.markers);
              $('#tutor-map-loading').hide();
            });
            $('#tutor-view-all-btn').on('click', function(event){
              mapControls.tutor.geoJsonLayer.refresh(mapDataURL['all']);
              $('#tutor-map-loading').show();
              $('#tutor-view-all-btn').hide();
            });
            
            //----------- SCDE MAP CONFIG -------------------------
            pageMaps.scde.addLayer(mapControls.scde.osm);
            pageMaps.scde.setView(new L.LatLng(0, 35), 6);
            mapControls.scde.layerControl.addTo(pageMaps.scde);
            mapControls.scde.geoJsonLayer = new L.GeoJSON.AJAX(mapDataURL['current'], {
              onEachFeature: layerOnEachFeature,
              filter: mapControls.scde.layerGeoJsonFilter
            });
            mapControls.scde.geoJsonLayer.on('data:loaded', function(){
              if ( mapControls.scde.markers == null || pageMaps.scde == null || mapControls.scde.geoJsonLayer == null ) { return; }
              mapControls.scde.markers.addLayer(mapControls.scde.geoJsonLayer);
              pageMaps.scde.addLayer(mapControls.scde.markers);
              $('#scde-map-loading').hide();
            });
            $('#scde-view-all-btn').on('click', function(event){
              mapControls.scde.geoJsonLayer.refresh(mapDataURL['all']);
              $('#scde-map-loading').show();
              $('#scde-view-all-btn').hide();
            });
            
            //----------- ESQAC MAP CONFIG -------------------------
            pageMaps.esqac.addLayer(mapControls.esqac.osm);
            pageMaps.esqac.setView(new L.LatLng(0, 35), 6);
            mapControls.esqac.layerControl.addTo(pageMaps.esqac);
            mapControls.esqac.geoJsonLayer = new L.GeoJSON.AJAX(mapDataURL['current'], {
              onEachFeature: layerOnEachFeature,
              filter: mapControls.esqac.layerGeoJsonFilter
            });
            mapControls.esqac.geoJsonLayer.on('data:loaded', function(){
              if ( mapControls.esqac.markers == null || pageMaps.esqac == null || mapControls.esqac.geoJsonLayer == null ) { return; }
              mapControls.esqac.markers.addLayer(mapControls.esqac.geoJsonLayer);
              pageMaps.esqac.addLayer(mapControls.esqac.markers);
              $('#esqac-map-loading').hide();
            });
            $('#esqac-view-all-btn').on('click', function(event){
              mapControls.esqac.geoJsonLayer.refresh(mapDataURL['all']);
              $('#esqac-map-loading').show();
              $('#esqac-view-all-btn').hide();
            });

            //----------- MATHS MAP CONFIG -------------------------
            pageMaps.maths.addLayer(mapControls.maths.osm);
            pageMaps.maths.setView(new L.LatLng(0, 35), 6);
            mapControls.maths.layerControl.addTo(pageMaps.maths);
            mapControls.maths.geoJsonLayer = new L.GeoJSON.AJAX(mapDataURL['current'], {
              onEachFeature: layerOnEachFeature,
              filter: mapControls.maths.layerGeoJsonFilter
            });
            mapControls.maths.geoJsonLayer.on('data:loaded', function(){
              if ( mapControls.maths.markers == null || pageMaps.maths == null || mapControls.maths.geoJsonLayer == null ) { return; }
              mapControls.maths.markers.addLayer(mapControls.maths.geoJsonLayer);
              pageMaps.maths.addLayer(mapControls.maths.markers);
              $('#maths-map-loading').hide();
            });
            $('#maths-view-all-btn').on('click', function(event){
              mapControls.maths.geoJsonLayer.refresh(mapDataURL['all']);
              $('#maths-map-loading').show();
              $('#maths-view-all-btn').hide();
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
        
        <div class='tab_container'>
          <div id='tab-tutor' class='tab first selected' data-id='tutor'>CSO</div>
          <div id='tab-scde' class='tab' data-id='scde'>SCDE</div>
          <div id='tab-esqac' class='tab' data-id='esqac'>ESQAC</div>
          <div id='tab-maths' class='tab last' data-id='maths'>MATHS</div>
          <section id='panel-tutor' class='tab-panel' style=''>
            #{tutorTabContent}
          </section>
          <section id='panel-scde' class='tab-panel' style='display:none;'>
            #{scdeTabContent}
          </section>
          <section id='panel-esqac' class='tab-panel' style='display:none;'>
            #{esqacTabContent}
          </section>
          <section id='panel-maths' class='tab-panel' style='display:none;'>
            #{mathTabContent}
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
    </html>
    "

    
    return html


  end # of report

end
