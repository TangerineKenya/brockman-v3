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
require_relative 'config.rb'
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
#groups.push({ 'db' => 'group-national_tablet_program', 'helper' => NtpReports, 'startYear' => 2014, 'endYear' => 2016 })
groups.push({ 'db' => 'group-national_tablet_program_test', 'helper' => NtpReports, 'startYear' => 2016, 'endYear' => 2016 })


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

  puts "\n- Retrieving Workflows: "
  taskStart = Time.now()

  workflowsRequest = couch.postRequest({
    :view => "byCollection",
    :params => { 
      "reduce" => false,
      "include_docs" => true
    },
    :data => {"keys" => ["workflow"]},
    :parseJson => true
  })


  workflows = {}

  workflowsRequest['rows'].each{ |e| 
    workflows[e['doc']['_id']] = e['doc']
  }
  puts "    #{workflows.length} Workflows Retrieved - (#{time_diff(Time.now(), taskStart)})"

  

  puts "\n- Caching Workflow Trips:"
  taskStart = Time.now()

  # workflowIds = []
  # workflows.each { |workflow|
  #   workflowId   = workflow['_id'] || ""
  #   workflowName = workflow['name'] || ""
  #   puts "\n    Workflow -> #{workflowName}"
  #   puts "      Id: #{workflowId}"
  #   subTaskStart = Time.now()

  #   if not workflow['reporting']['preProcess']
  #     puts "      [SKIP] Pre-Processing Disabled - (#{time_diff(Time.now(), subTaskStart)})"
  #     next
  #   end

    # add workflow ID to the list of those processed
  #  workflowIds.push workflowId

  #   tripsRequest = JSON.parse(couch.postRequest({
  #     :view => "tutorTrips",
  #     :params => {"reduce" => false},
  #     :data => {"keys" => ["workflow-#{workflowId}"]}
  #   }))
  #   hTripIds = {}
  #   tripsRequest['rows'].each { |row| hTripIds[row['value']] = true }
  #   aTripIds      = hTripIds.keys
  #   totalTripIds  = aTripIds.length
    
  #   puts "      # Trips: #{totalTripIds}"
  #   print "      "

  #   (0..totalTripIds).step(CHUNK_SIZE).each { | chunkIndex |
  #     idChunk = aTripIds.slice(chunkIndex, chunkIndex + CHUNK_SIZE)
  #     couch.postRequest({
  #       :view   => "spirtRotut",
  #       :params => { "group" => true },
  #       :cache  => true,
  #       :data   => { "keys" => idChunk }
  #     })
  #     print "*"
  #   }
    
  #   puts "\n      [COMPLETE] Workflow Processed - (#{time_diff(Time.now(), subTaskStart)})"
  # }
  
  puts "\n   [COMPLETE] Caching Workflow Trips - (#{time_diff(Time.now(), taskStart)})"

#
#
# => BEGIN Pre-processing data for reports
#
#

 



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
#    (1..12).each { |month|
    (1..2).each { |month|
    
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

      monthKeys = ["year#{year}month#{month}"]
      tripsFromMonth = couch.postRequest({ 
        :view   => "tutorTrips", 
        :data   => { "keys"   => monthKeys }, 
        :params => { "reduce" => false }, 
        :categoryCache => true,
        :parseJson => true
      })

      tripIds = tripsFromMonth['rows'].map{ |e| e['value'] }

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

      #
      # Get chunks of trips and work on the result
      #

      #print "      Filtering Valid Visits... "
      tripKeyChunks.each { | tripKeys |

        # get the real data
        tripsResponse = couch.postRequest({
          :view => "spirtRotut",
          :params => { "group" => true },
          :data => { "keys" => tripKeys },
          :parseJson => true,
          :cache => true
        } )
        tripRows = tripsResponse['rows']

        #puts "Processing Chunk:  #{tripRows.length}"

        # Process each Trip result record in chunk
        for trip in tripRows
          helper.processTrip(trip, monthData, templates, workflows) if helper
        end

      }

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



