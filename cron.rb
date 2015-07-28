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


header = <<END

Brockman presents
               |                       ,---.               |    
,---.,---.,---.|---.,---.    ,---.,---.|__. ,---.,---.,---.|---.
|    ,---||    |   ||---'    |    |---'|    |    |---'`---.|   |
`---'`---^`---'`   '`---'    `    `---'`    `    `---'`---'`   '
END

puts header

dbs             = [ 'group-national_tablet_program' ] #[ 'group-tangent_6m_complete' ] #'group-tangent_1m_complete', 'group-tangent_2m_complete' ]
years           = [ 2014, 2015, 2016 ]
months          = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 ]
workflowIds     = ["00b0a09a-2a9f-baca-2acb-c6264d4247cb","c835fc38-de99-d064-59d3-e772ccefcf7d"]
subjectLegend   = { "english_word" => "English", "word" => "Kiswahili", "operation" => "Maths" } 

#
#   Time Variables for tracking processing time
#
cronStart     = Time.now()
dbStart       = nil
taskStart     = nil
subTaskStart  = nil

CHUNK_SIZE  = 1000

dbs.each { |db|

  puts "\nStarting DB: #{db}"
  dbStart = Time.now()

  couch = Couch.new({
    :host      => $settings[:dbHost],
    :login     => $settings[:login],
    :designDoc => $settings[:designDoc],
    :db        => db
  })

  puts "\n- Caching Trips: "
  taskStart = Time.now()

  workflowIds.each { |workflowId|

    puts "\n    Processing Workflow: #{workflowId}"
    subTaskStart = Time.now()

    tripsRequest = JSON.parse(couch.postRequest({
      :view => "tutorTrips",
      :params => {"reduce" => false},
      :data => {"keys" => ["workflow-#{workflowId}"]}
    }))
    hTripIds = {}
    tripsRequest['rows'].each { |row| hTripIds[row['value']] = true}
    aTripIds      = hTripIds.keys
    totalTripIds  = aTripIds.length
    
    puts "      #{totalTripIds} Trips"
    print "      "

    (0..totalTripIds).step(CHUNK_SIZE).each { | chunkIndex |
      idChunk = aTripIds.slice(chunkIndex, chunkIndex + CHUNK_SIZE)
      couch.postRequest({
        :view   => "spirtRotut",
        :params => { "group" => true },
        :cache  => true,
        :data   => { "keys" => idChunk }
      })
      print "M"
    }
    
    puts "\n      Workflow Processing Completed - (#{time_diff(Time.now(), subTaskStart)})"
  }
  
  puts "\n    Caching Trips Completed - (#{time_diff(Time.now(), taskStart)})"

#
#
# => BEGIN Pre-processing data for reports
#
#

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



  #
  # Process schools and setup data structure
  #
  puts "\n- Processing Schools"
  taskStart = Time.now()

  schoolList = couch.getRequest({ 
    :doc => "school-list", 
    :parseJson => true 
  })

  # define scope for result
  resultTemplate                                        ||= {}
  resultTemplate['visits']                              ||= {}
  resultTemplate['visits']['byCounty']                  ||= {}
  resultTemplate['visits']['national']                  ||= {}
  resultTemplate['visits']['national']['visits']        ||= 0
  resultTemplate['visits']['national']['quota']         ||= 0
  resultTemplate['visits']['national']['compensation']  ||= 0
  resultTemplate['visits']['national']['fluency']       ||= {}

  resultTemplate['users']           ||= {}  #stores list of all users and zone associations
  resultTemplate['users']['all']    ||= {}  #stores list of all users

  resultTemplate['compensation']               ||= {}
  resultTemplate['compensation']['byCounty']   ||= {}
  resultTemplate['compensation']['national']   ||= 0

  # define scope or the geoJSON files
  geoJSON               ||= {}
  geoJSON['byCounty']   ||= {}

  #
  # Retrieve Shool Locations and Quotas
  #

  # Init the data structures based on the school list 
  schoolList['counties'].map { | countyName, county |
    countyName = countyTranslate( countyName.downcase )
    resultTemplate['visits']['byCounty'][countyName]                  ||= {}
    resultTemplate['visits']['byCounty'][countyName]['zones']         ||= {}
    resultTemplate['visits']['byCounty'][countyName]['visits']        ||= 0
    resultTemplate['visits']['byCounty'][countyName]['quota']         ||= 0
    resultTemplate['visits']['byCounty'][countyName]['compensation']  ||= 0
    resultTemplate['visits']['byCounty'][countyName]['fluency']       ||= {}

    resultTemplate['visits']['byCounty'][countyName]['quota'] = county['quota']

    #manually flatten out the subCounty data level
    county['subCounties'].map { | subCountyName, subCounty | 
      subCounty['zones'].map { | zoneName, zone |
        zoneName = zoneTranslate(zoneName.downcase)

        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]                   ||= {}
        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]['trips']          ||= []
        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]['visits']         ||= 0
        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]['quota']          ||= 0
        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]['compensation']   ||= 0
        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]['fluency']        ||= {}

        resultTemplate['visits']['byCounty'][countyName]['zones'][zoneName]['quota']  += zone['quota'].to_i
        resultTemplate['visits']['national']['quota']                                 += zone['quota'].to_i

        #init container for users
        resultTemplate['users'][countyName]                   ||= {}
        resultTemplate['users'][countyName][zoneName]         ||= {}

        #init geoJSON Containers
        geoJSON['byCounty'][countyName]         ||= {}
        geoJSON['byCounty'][countyName]['data'] ||= []
      }
    } 
  }
  puts "    Processing Schools Completed - (#{time_diff(Time.now(), taskStart)})"


  #
  # Retrieve and Filter All Users
  #
  puts "\n- Processing Users"
  taskStart = Time.now()

  userDocs = couch.getRequest({
    :doc => "_all_docs",
    :params => { 
      "startkey" => "user-".to_json,
      "include_docs" => true
    },
    :parseJson => true
  })

  puts "    #{userDocs['rows'].size} Total Users"
  #associate users with their county and zone for future processing
  userDocs['rows'].map{ | user | 
    unless user['doc']['location'].nil?
      location = user['doc']['location']
      county = countyTranslate(location['County'].downcase) if !location['County'].nil?
      zone = zoneTranslate(location['Zone'].downcase) if !location['Zone'].nil?

      #verify that the user has a zone and county associated
      if !county.nil? && !zone.nil?
        username                                          = user['doc']['name']
        resultTemplate['users']['all']                  ||= {}
        resultTemplate['users'][county]                 ||= {}
        resultTemplate['users'][county][zone]           ||= {}
        resultTemplate['users'][county][zone][username] = true

        resultTemplate['users']['all'][username]                            ||= {}
        resultTemplate['users']['all'][username]['data']                    = user['doc']

        resultTemplate['users']['all'][username]['target']                  ||= {}      # container for target zone visits
        resultTemplate['users']['all'][username]['target']['visits']        ||= 0
        resultTemplate['users']['all'][username]['target']['compensation']  ||= 0

        resultTemplate['users']['all'][username]['other']                   ||= {}      # container for non-target zone visits

        resultTemplate['users']['all'][username]['total']                   ||= {}      # container for visit and compensation totals
        resultTemplate['users']['all'][username]['total']['visits']         ||= 0       # total visits across zones
        resultTemplate['users']['all'][username]['total']['compensation']   ||= 0       # total compensation across zones
        resultTemplate['users']['all'][username]['flagged']                 ||= false   # alert to visits outside of primary zone
      end
    end
  }
  puts "    Processing Users Completed - (#{time_diff(Time.now(), taskStart)})"
  
  puts "\n- Processing Tutor Trips By Month"
  taskStart = Time.now()

  years.each { |year| 
    months.each { |month|

      puts "  * #{month}/#{year}"
      subTaskStart = Time.now()

      aggregateDocId = "report-aggregate-year#{year}month#{month}"
      aggregateGeoDocId = "report-aggregate-geo-year#{year}month#{month}"

      #duplicate the resultTemplate to store this months data
      result = cloneDeep(resultTemplate)
      geojson = {}
      geojson['data'] = []

      # Check to see if the aggregate doc already exists
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
        result['_rev'] = aggDoc['_rev']
      end

      # Check to see if the aggregate geo doc already exists for each county
      
      schoolList['counties'].map { | countyName, county |
        countyName = countyTranslate( countyName.downcase )
        begin
          aggGeoDoc = couch.getRequest({ 
            :doc => "#{aggregateGeoDocId}-#{Base64.urlsafe_encode64(countyName)}", 
            :parseJson => true 
          })
        rescue => e
          # the doc doesn't already exist
          aggGeoDoc = {}
        end

        if aggGeoDoc.has_key?('_rev')
          geoJSON['byCounty'][countyName]['_rev'] = aggGeoDoc['_rev']
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

      # if workflows specified, filter trips to those workflows
      if workflowIds != "all"

        workflowKey = workflowIds.map{ |s| "workflow-#{s}" }
        allRows = []
        workflowIds.each { |workflowId|

          workflowResponse = couch.postRequest({ 
            :view => "tutorTrips", 
            :data => { "keys" => ["workflow-#{workflowId}"] }, 
            :params => { "reduce" => false },
            :parseJson => true,
            :categoryCache => true
          } )

          allRows += workflowResponse['rows']
        }

        tripsFromWorkflow = allRows.map{ |e| e['value'] }
        tripIds           = tripIds & tripsFromWorkflow

      end

      # remove duplicates (of which there are many)
      tripKeys = tripIds.uniq

      puts "      #{tripKeys.size} Trips"

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

      print "      Filtering Valid Visits... "
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

        #
        # filter rows
        #

        tripRows = tripRows.select { | row |
          longEnough = ( row['value']['maxTime'].to_i - row['value']['minTime'].to_i ) / 1000 / 60 >= 20
          longEnough
        }

        #
        # result['visits']
        #

        for sum in tripRows

          next if sum['value']['zone'].nil?
          next if sum['value']['county'].nil? 
          
          zoneName   = zoneTranslate(sum['value']['zone'].downcase)
          countyName = countyTranslate(sum['value']['county'].downcase)
          username   = sum['value']['user'].downcase

          # prepare the geojson doc for the map
          if !sum['value']['gpsData'].nil?
            point = sum['value']['gpsData']

            minuteDuration = (sum['value']['maxTime'].to_i - sum['value']['minTime'].to_i ) / 1000 / 60 / 3600 #TODO: check back on the validity of this
            
            if !groupTimeZone.nil?
              startDate = Time.at(sum['value']['minTime'].to_i / 1000).strftime("%Y %b %d %H:%M")
            else 
              startDate = Time.at(sum['value']['minTime'].to_i / 1000).strftime("%Y %b %d %H:%M")
            end

            point['properties'] = [
              { 'label' => 'Date',            'value' => startDate },
              { 'label' => 'Subject',         'value' => subjectLegend[sum['value']['subject']] },
              { 'label' => 'Lesson duration', 'value' => "#{minuteDuration} min." },
              { 'label' => 'Zone',            'value' => sum['value']['zone'] },
              { 'label' => 'TAC tutor',       'value' => sum['value']['user'] },
              { 'label' => 'Lesson Week',     'value' => sum['value']['week'] },
              { 'label' => 'Lesson Day',      'value' => sum['value']['day'] }
            ]

            geoJSON['byCounty'][countyName]['data'].push point
          end

          
          
          #puts "---#{countyName}---#{zoneName}---#{sum['id']}---#{username}---"
          #skip these steps if either the county or zone are no longer in the primary list 
          next if result['visits']['byCounty'][countyName].nil?
          next if result['visits']['byCounty'][countyName]['zones'].nil?
          next if result['visits']['byCounty'][countyName]['zones'][zoneName].nil?
          next if result['visits']['byCounty'][countyName]['zones'][zoneName]['visits'].nil?

          result['visits']['byCounty'][countyName]['zones'][zoneName]['trips'].push sum['id']

          next if result['users']['all'][username].nil?

          if !result['users'][countyName][zoneName][username].nil?
            result['users']['all'][username]['target']['visits']  += 1

          else

            result['users']['all'][username]['other'][countyName]                             ||= {}
            result['users']['all'][username]['other'][countyName][zoneName]                   ||= {}
            result['users']['all'][username]['other'][countyName][zoneName]['visits']         ||= 0
            result['users']['all'][username]['other'][countyName][zoneName]['compensation']   ||= 0

            result['users']['all'][username]['flagged']                                 = true
            result['users']['all'][username]['other'][countyName][zoneName]['visits']   += 1
          end

          result['users']['all'][username]['total']['visits']   += 1

          result['visits']['national']['visits']                                  += 1
          result['visits']['byCounty'][countyName]['visits']                      += 1 
          result['visits']['byCounty'][countyName]['zones'][zoneName]['visits']   += 1

          #
          # process fluency data
          #
          
          if !sum['value']['itemsPerMinute'].nil? and
             !sum['value']['subject'].nil? and
             sum['value']['subject'] != ""

            itemsPerMinute = sum['value']['itemsPerMinute']
            benchmarked    = sum['value']['benchmarked']
            met            = sum['value']['metBenchmark']

            subject = sum['value']['subject']

            if !reportSettings['fluency']['subjects'].include?(subject)
              reportSettings['fluency']['subjects'].push subject
            end

            #pushUniq reportSettings['fluency']['subjects'], subject, subjectsExists

            total = 0
            itemsPerMinute.each { | ipm | total += ipm }

            result['visits']['national']['fluency'][subject]                  ||= {}
            result['visits']['national']['fluency'][subject]['sum']           ||= 0
            result['visits']['national']['fluency'][subject]['size']          ||= 0
            result['visits']['national']['fluency'][subject]['metBenchmark']  ||= 0

            result['visits']['national']['fluency'][subject]['sum']           += total
            result['visits']['national']['fluency'][subject]['size']          += benchmarked
            result['visits']['national']['fluency'][subject]['metBenchmark']  += met

            result['visits']['byCounty'][countyName]['fluency'][subject]                  ||= {}
            result['visits']['byCounty'][countyName]['fluency'][subject]['sum']           ||= 0
            result['visits']['byCounty'][countyName]['fluency'][subject]['size']          ||= 0
            result['visits']['byCounty'][countyName]['fluency'][subject]['metBenchmark']  ||= 0

            result['visits']['byCounty'][countyName]['fluency'][subject]['sum']           += total
            result['visits']['byCounty'][countyName]['fluency'][subject]['size']          += benchmarked
            result['visits']['byCounty'][countyName]['fluency'][subject]['metBenchmark']  += met

            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]                  ||= {}
            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]['sum']           ||= 0
            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]['size']          ||= 0
            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]['metBenchmark']  ||= 0

            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]['sum']           += total
            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]['size']          += benchmarked
            result['visits']['byCounty'][countyName]['zones'][zoneName]['fluency'][subject]['metBenchmark']  += met

          end
          
        end
      }

      # Calculate the user compensation
      #
      puts "Calculating Compensation...\n"
      result['users']['all'].map{ | userName, user |

        location = result['users']['all'][userName]['data']['location']

        countyName  = countyTranslate(location['County'].downcase) if !location['County'].nil?
        zoneName    = zoneTranslate(location['Zone'].downcase) if !location['Zone'].nil?


        #skip these steps if either the county or zone are no longer in the primary list 
        next if result['visits']['byCounty'][countyName].nil?
        next if result['visits']['byCounty'][countyName]['zones'].nil?
        next if result['visits']['byCounty'][countyName]['compensation'].nil?
        next if result['visits']['byCounty'][countyName]['zones'][zoneName].nil?
        next if result['visits']['byCounty'][countyName]['zones'][zoneName]['compensation'].nil?


        #ensure that the user has a county and zone assigned that exist
        if !countyName.nil? && !zoneName.nil? && (result['users']['all'][userName]['total']['visits'] > 0)


          # handle compensation for visits outside the assigned zone 
          if user['flagged'] == true
            user['other'].map{ | altCountyName, altCounty |
              altCounty.map{ | altZoneName, altZone |

                result['users'][altCountyName][altZoneName][userName] = true
                
                completePct = (altZone['visits'] + 0.0) / result['visits']['byCounty'][altCountyName]['zones'][altZoneName]['quota']
                compensation = (((completePct > 1) ? 1 : completePct) * 6000).round(2)

                altZone['compensation']                                   += compensation
                result['users']['all'][userName]['total']['compensation'] += compensation

                result['visits']['national']['compensation']                                  += compensation
                result['visits']['byCounty'][altCountyName]['compensation']                      += compensation
                result['visits']['byCounty'][altCountyName]['zones'][altZoneName]['compensation']   += compensation
              }
            }

            #cover the situation where no visits have been completed in the target zone but have elsewhere - need to cover data upload
            if (user['other'].length > 0) && (user['target']['visits'] == 0)

              result['users']['all'][userName]['target']['compensation'] += 300;
              result['users']['all'][userName]['total']['compensation']  += 300;

              result['visits']['national']['compensation']                                  += 300
              result['visits']['byCounty'][countyName]['compensation']                      += 300
              result['visits']['byCounty'][countyName]['zones'][zoneName]['compensation']   += 300
            end 

          else 
            completePct   = (user['target']['visits'] + 0.0) / result['visits']['byCounty'][countyName]['zones'][zoneName]['quota']
            compensation  = (((completePct > 1) ? 1 : completePct) * 6000 + 300).round(2)
            
            result['users']['all'][userName]['target']['compensation'] += compensation;
            result['users']['all'][userName]['total']['compensation']  += compensation;


            result['visits']['national']['compensation']                                  += compensation
            result['visits']['byCounty'][countyName]['compensation']                      += compensation
            result['visits']['byCounty'][countyName]['zones'][zoneName]['compensation']   += compensation

          end

        end
      }

      #
      # Saving the generated results back to the server
      couch.putRequest({ 
        :doc => "#{aggregateDocId}", 
        :data => result 
      })


      geoJSON['byCounty'].map { | countyName, countyData |
        
        #Saving the generated Geo Results back to the server
        couch.putRequest({ 
          :doc => "#{aggregateGeoDocId}-#{Base64.urlsafe_encode64(countyName)}", 
          :data => countyData 
        })
      
      }

      puts "      Month Completed - (#{time_diff(Time.now(), subTaskStart)})"

    }
  }

  puts "\n    Processing Tutor Trips by Month Completed - (#{time_diff(Time.now(), taskStart)})" 

  # ensure that the lisyt of subjects doens't contain duplicates
  reportSettings['fluency']['subjects'] = reportSettings['fluency']['subjects'].uniq

  #
  # Saving the aggregate report settings in case of modification
  couch.putRequest({ 
    :doc => "report-aggregate-settings", 
    :data => reportSettings 
  })

  puts "\n  DB Processing Completed - (#{time_diff(Time.now, dbStart)})"

  

}

puts "\nCron Job Completed - (#{time_diff(Time.now(), cronStart)})"



