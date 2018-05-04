#! /usr/bin/env ruby

#
# This file is to be run via crontab
#

#
# What is cached
# Three requests
# 1. spritRotut by tripId
# 2. tutorTrips by year+month
# 3. tutorTrips by workflowId
# The union of the latter two will give a list of 
require 'base64'
require_relative 'settings.rb'
require_relative 'helpers/Couch'
require_relative 'helpers/CouchIterator'
require_relative 'utilities/cloneDeep'
require_relative 'utilities/countyTranslate'
require_relative 'utilities/pushUniq'
require_relative 'utilities/timestamp'
require_relative 'utilities/zoneTranslate'

require_relative 'cronSupport/ntpReports'


header = <<END

Brockman presents
               |                       ,---.               |    
,---.,---.,---.|---.,---.    ,---.,---.|__. ,---.,---.,---.|---.
|    ,---||    |   ||---'    |    |---'|    |    |---'`---.|   |
`---'`---^`---'`   '`---'    `    `---'`    `    `---'`---'`   '
END

puts header

groups = []
groups.push({ 'db' => 'tusome-v3-prod', 'helper' => NtpReports, 'startYear' => 2018, 'endYear' => 2018 })


#
#   Time Variables for tracking processing time
#
cronStart     = Time.now()
dbStart       = nil
workflowStart = nil
taskStart     = nil
subTaskStart  = nil

CHUNK_SIZE  = 1000

groups.each { |group|

  #
  #  Prep for preprocessing the group
  #

  # Determine DB and init Couch Connection
  db = group["db"] || ""
  helper = group["helper"] || nil

  puts "\nStarting DB: #{db}"
  dbStart = Time.now()

  couch = Couch.new({
    :host      => $settings[:dbHost],
    :login     => $settings[:login],
    :designDoc => $settings[:designDoc],
    :db        => db
  })

  #
  # get Group settings - for time zone calculation
  #
  groupSettings = couch.getRequest({ :doc => 'settings', :parseJson => true })
  groupTimeZone = groupSettings['timeZone'] 

  #
  # get report aggregate settings
  #
  begin
    reportSettings = couch.getRequest({ :doc => "report-aggregate-settings", :parseJson => true })
  rescue => e
    # the doc doesn't already exist
    reportSettings                          ||= {}
    reportSettings['fluency']               ||= {}
    reportSettings['fluency']['subjects']   ||= []
  end

  # Determine if there is a helper class and init it
  if group["helper"]
    helper = group["helper"].new(:couch => couch, :timezone => groupTimeZone, :reportSettings => reportSettings)
  end

  #
  #  Identify Workflows to Process
  #

  workflows = {}
  workflowIds = []

  groupSettings['workflows'].map{ |e| 
    workflows[e['id']] = e
    workflowIds.push e['id']
  }
  #puts "Workflow #{workflows}"
  #puts "    #{workflows.length} Workflows Retrieved - (#{time_diff(Time.now(), taskStart)})"

  #find a way to pull this data from couchdb - probal store in the settings file?
  #workflowIds = ['Gradethreeobservationtool','class-12-lesson-observation-with-pupil-books','maths-teachers-observation-tool','maths-grade3','tusome-classroom-observation-tool-for-sne']

  #
  # Process locations and setup data structure
  #
  templates                       ||= {}
  templates['result']             ||= {}
  templates['geoJSON']            ||= {}
  templates['locationBySchool']   ||= {}
  templates['users']              ||= {}


  puts "\n- Processing Locations"
  taskStart = Time.now()

  templates = helper.processLocations(templates) if helper

  puts "   [COMPLETE] Processing Schools  - (#{time_diff(Time.now(), taskStart)})"

  #
  # Retrieve and Filter All Users
  #
  puts "\n- Processing Users"
  taskStart = Time.now()

  templates = helper.processUsers(templates) if helper
  
  puts "   [COMPLETE] Processing Users - (#{time_diff(Time.now(), taskStart)})"

  #
  # Processing Trips By Month
  #

  puts "\n- Processing Tutor Trips By Month"
  taskStart = Time.now()


  (group["startYear"]..group["endYear"]).each { |year| 
    #(1..12).each { |month|
    (4..5).each { |month|

      helper.resetSkippedCount() if helper

      puts "  * #{month}/#{year}"
      subTaskStart = Time.now()

      aggregateDocId = "report-aggregate-year#{year}month#{month}"
      aggregateGeoDocId = "report-aggregate-geo-year#{year}month#{month}"

      #duplicate the resultTemplate to store this months data
      monthData               = {}
      monthData['result']     = cloneDeep(templates['result'])
      monthData['geoJSON']    = cloneDeep(templates['geoJSON'])

      # Check to see if the aggregate doc already exists - need for doc update
      begin
        aggDoc = couch.getRequest({ 
          :doc => "#{aggregateDocId}", 
          :parseJson => true 
        })
      rescue => e
        # the doc doesn't already exist
        aggDoc = {}
      end

      if aggDoc.has_key?('_rev')
        monthData['result']['_rev'] = aggDoc['_rev']
      end

      # Check to see if the aggregate geo doc already exists for each county - needed for doc update
      monthData['geoJSON']['byCounty'].map { | countyId, county |
        begin
          aggGeoDoc = couch.getRequest({ 
            :doc => "#{aggregateGeoDocId}-#{countyId}", 
            :parseJson => true 
          })
        rescue => e
          # the doc doesn't already exist
          aggGeoDoc = {}
        end

        if aggGeoDoc.has_key?('_rev')
          monthData['geoJSON']['byCounty'][countyId]['_rev'] = aggGeoDoc['_rev']
        end
      }

      workflowIds.each{ |workflowId|
        puts "   Processing Workflow - #{workflowId}"
        monthKeys = ["year#{year}month#{month}formId#{workflowId}"]
        
        tripsFromMonth = couch.postRequest({ 
          :view   => "completedTripsByYearAndMonth", 
          :data   => { "keys"   => monthKeys }, 
          :params => { "reduce" => false }, 
          :categoryCache => true,
          :parseJson => true
        })

        tripIds = tripsFromMonth['rows'].map{ |e| e['value'] }
        # puts "trips #{tripIds}"
        # remove duplicates
        tripKeys = tripIds.uniq

        puts "      # Trips: #{tripKeys.size}"

         # break trip keys into chunks
        tripKeyChunks = tripKeys.each_slice(CHUNK_SIZE).to_a

        # hash for optimization
        subjectsExists = {}
        zoneCountyExists = {
          'all' => {}
        }

        tripRows = tripsFromMonth['rows']
        
        # Process each Trip result record in chunk
        for trip in tripRows
          helper.processTrip(trip, monthData, templates, workflows) if helper
          #staff trips
          #helper.processStaffTrip(trip, monthData, templates, workflows) if helper
        end
      }
      #post processing
      #puts "Processing Compensation"
      #helper.postProcessTrips(monthData, templates) if helper
      #end

      puts "      # Skipped: #{helper.getSkippedCount()}"

      #
      # Saving the generated result back to the server
      puts "Putting result Doc: #{aggregateDocId}"
      couch.putRequest({ 
        :doc => "#{aggregateDocId}", 
        :data => monthData['result'] 
      })


      monthData['geoJSON']['byCounty'].map { | countyId, countyData |
        
        #Saving the generated Geo result back to the server
        couch.putRequest({ 
          :doc => "#{aggregateGeoDocId}-#{countyId}", 
          :data => countyData 
        })
      
      }

      puts "      Month Completed - (#{time_diff(Time.now(), subTaskStart)})"

    }
  }

  puts "\n    Processing Tutor Trips by Month Completed - (#{time_diff(Time.now(), taskStart)})" 

  # ensure that the lisyt of subjects doens't contain duplicates
  #reportSettings['fluency']['subjects'] = reportSettings['fluency']['subjects'].uniq

  #
  # Saving the aggregate report settings in case of modification
  # couch.putRequest({ 
  #   :doc => "report-aggregate-settings", 
  #   :data => reportSettings 
  # })

  puts "\n  DB Processing Completed - (#{time_diff(Time.now, dbStart)})"  

}

puts "\nCron Job Completed - (#{time_diff(Time.now(), cronStart)})"



