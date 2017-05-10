# encoding: utf-8

#require 'rest-client'
#require 'json'
#require_relative "Stash"
require_relative "../utilities/TimeDifference"

class NtpReports

  def initialize( options = {} )

    @couch          = options[:couch]
    @timezone       = options[:timezone]
    @reportSettings = options[:reportSettings]
    #@validation = options[:validation]
    puts "NtpReports: Init Complete"

    @locationList = nil
    @tripsSkipped = 0
    @subjectLegend   = { "english_word" => "English", "word" => "Kiswahili", "operation" => "Maths" } 

  end # of initialize

  # Process locations
  def processLocations(templates)
    
    @locationList = @couch.getRequest({ 
      :doc => "location-list", 
      :parseJson => true 
    })

    templates['locationBySchool']                              ||= {}
    templates['locationByZone']                                ||= {}

    # define scope for result
    templates                                                  ||= {}
    templates['result']['visits']                              ||= {}
    templates['result']['visits']['byCounty']                  ||= {}
    templates['result']['visits']['national']                  ||= {}
    templates['result']['visits']['national']['visits']        ||= 0
    templates['result']['visits']['national']['quota']         ||= 0
    templates['result']['visits']['national']['numTeachers']   ||= 0
    templates['result']['visits']['national']['compensation']  ||= 0
    templates['result']['visits']['national']['fluency']                   ||= {}
    templates['result']['visits']['national']['fluency']['class']          ||= {}
    templates['result']['visits']['national']['fluency']['class'][1]       ||= {}
    templates['result']['visits']['national']['fluency']['class'][2]       ||= {}
    templates['result']['visits']['national']['fluency']['class'][3]       ||= {}

    templates['result']['visits']['scde']                              ||= {}
    templates['result']['visits']['scde']['national']                  ||= {}
    templates['result']['visits']['scde']['national']['visits']        ||= 0
    templates['result']['visits']['scde']['national']['quota']         ||= 0
    templates['result']['visits']['scde']['national']['numTeachers']   ||= 0
    templates['result']['visits']['scde']['national']['compensation']  ||= 0

    templates['result']['visits']['esqac']                              ||= {}
    templates['result']['visits']['esqac']['byCounty']                  ||= {}
    templates['result']['visits']['esqac']['national']                  ||= {}
    templates['result']['visits']['esqac']['national']['visits']        ||= 0
    templates['result']['visits']['esqac']['national']['quota']         ||= 0
    templates['result']['visits']['esqac']['national']['numTeachers']   ||= 0
    templates['result']['visits']['esqac']['national']['compensation']  ||= 0
    templates['result']['visits']['esqac']['national']['fluency']       ||= {}

    templates['result']['visits']['priede']                              ||= {}
    templates['result']['visits']['priede']['byCounty']                  ||= {}
    templates['result']['visits']['priede']['national']                  ||= {}
    templates['result']['visits']['priede']['national']['visits']        ||= 0
    templates['result']['visits']['priede']['national']['quota']         ||= 0
    templates['result']['visits']['priede']['national']['numTeachers']   ||= 0
    templates['result']['visits']['priede']['national']['compensation']  ||= 0
    templates['result']['visits']['priede']['national']['fluency']       ||= {}

    templates['result']['visits']['moe']                              ||= {}
    templates['result']['visits']['moe']['byCounty']                  ||= {}
    templates['result']['visits']['moe']['national']                  ||= {}
    templates['result']['visits']['moe']['national']['visits']        ||= 0
    templates['result']['visits']['moe']['national']['quota']         ||= 0
    templates['result']['visits']['moe']['national']['numTeachers']   ||= 0
    templates['result']['visits']['moe']['national']['compensation']  ||= 0
    templates['result']['visits']['moe']['national']['fluency']       ||= {}

    #Maths observations
    templates['result']['visits']['maths']                              ||= {}
    templates['result']['visits']['maths']['byCounty']                  ||= {}
    templates['result']['visits']['maths']['national']                  ||= {}
    templates['result']['visits']['maths']['national']['visits']        ||= 0
    templates['result']['visits']['maths']['national']['quota']         ||= 0
    templates['result']['visits']['maths']['national']['numTeachers']   ||= 0
    templates['result']['visits']['maths']['national']['compensation']  ||= 0
    templates['result']['visits']['maths']['national']['fluency']       ||= {}

    templates['result']['users']           ||= {}  #stores list of all users and zone associations
    templates['result']['users']['all']    ||= {}  #stores list of all users

    templates['result']['compensation']               ||= {}
    templates['result']['compensation']['byCounty']   ||= {}
    templates['result']['compensation']['national']   ||= 0

    # define scope or the geoJSON files
    templates['geoJSON']               ||= {}
    templates['geoJSON']['byCounty']   ||= {}

    #
    # Retrieve Shool Locations and Quotas
    #

    # Init the data structures based on the school list 
    @locationList['locations'].map { | countyId, county |
      templates['result']['visits']['byCounty'][countyId]                  ||= {}
      templates['result']['visits']['byCounty'][countyId]['name']          ||= county['label']
      templates['result']['visits']['byCounty'][countyId]['subCounties']   ||= {}
      templates['result']['visits']['byCounty'][countyId]['zones']         ||= {}
      templates['result']['visits']['byCounty'][countyId]['visits']        ||= 0
      templates['result']['visits']['byCounty'][countyId]['quota']         ||= 0
      templates['result']['visits']['byCounty'][countyId]['numTeachers']   ||= 0
      templates['result']['visits']['byCounty'][countyId]['compensation']  ||= 0
      templates['result']['visits']['byCounty'][countyId]['fluency']       ||= {}
      templates['result']['visits']['byCounty'][countyId]['fluency']['class']          ||= {}
      templates['result']['visits']['byCounty'][countyId]['fluency']['class'][1]       ||= {}
      templates['result']['visits']['byCounty'][countyId]['fluency']['class'][2]       ||= {}
      templates['result']['visits']['byCounty'][countyId]['fluency']['class'][3]       ||= {}

      # templates['result']['visits']['esqac']['byCounty'][countyId]                  ||= {}
      # templates['result']['visits']['esqac']['byCounty'][countyId]['name']          ||= county['label']
      # templates['result']['visits']['esqac']['byCounty'][countyId]['zones']         ||= {}
      # templates['result']['visits']['esqac']['byCounty'][countyId]['visits']        ||= 0
      # templates['result']['visits']['esqac']['byCounty'][countyId]['quota']         ||= 0
      # templates['result']['visits']['esqac']['byCounty'][countyId]['compensation']  ||= 0
      # templates['result']['visits']['esqac']['byCounty'][countyId]['fluency']       ||= {}

      templates['result']['visits']['byCounty'][countyId]['esqac']               ||= {}
      templates['result']['visits']['byCounty'][countyId]['esqac']['visits']     ||= 0
      templates['result']['visits']['byCounty'][countyId]['esqac']['quota']      ||= 0
      
      templates['result']['visits']['byCounty'][countyId]['priede']               ||= {}
      templates['result']['visits']['byCounty'][countyId]['priede']['visits']     ||= 0
      templates['result']['visits']['byCounty'][countyId]['priede']['quota']      ||= 0

      templates['result']['visits']['byCounty'][countyId]['moe']               ||= {}
      templates['result']['visits']['byCounty'][countyId]['moe']['visits']     ||= 0
      templates['result']['visits']['byCounty'][countyId]['moe']['quota']      ||= 0

      templates['result']['visits']['byCounty'][countyId]['scde']               ||= {}
      templates['result']['visits']['byCounty'][countyId]['scde']['visits']     ||= 0
      templates['result']['visits']['byCounty'][countyId]['scde']['quota']      ||= 0
      

      templates['result']['visits']['byCounty'][countyId]['quota']    = county['quota']
      
      #maths
      templates['result']['visits']['maths']['byCounty'][countyId]                              ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['name']                      ||= county['label']
      templates['result']['visits']['maths']['byCounty'][countyId]['subCounties']               ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['zones']                     ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['visits']                    ||= 0
      templates['result']['visits']['maths']['byCounty'][countyId]['quota']                     ||= 0
      templates['result']['visits']['maths']['byCounty'][countyId]['numTeachers']               ||= 0
      templates['result']['visits']['maths']['byCounty'][countyId]['compensation']              ||= 0
      templates['result']['visits']['maths']['byCounty'][countyId]['fluency']                   ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['fluency']['class']          ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][1]       ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][2]       ||= {}
      templates['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][3]       ||= {}

      templates['result']['visits']['maths']['byCounty'][countyId]['quota']    = county['quota']

      #manually flatten out the subCounty data level
      county['children'].map { | subCountyId, subCounty | 
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]             ||= {}
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['name']     ||= subCounty['label']
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['zones']    ||= [] #subCounty['children'].map { | zoneId | }

        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']             ||= {}
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['trips']    ||= []
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['visits']   ||= 0
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['quota']    ||= 0
        

        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']             ||= {}
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['trips']    ||= []
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['visits']   ||= 0
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['quota']    ||= 0

        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']             ||= {}
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['trips']    ||= []
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['visits']   ||= 0
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['quota']    ||= 0

        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['moe']             ||= {}
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['moe']['trips']    ||= []
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['moe']['visits']   ||= 0
        templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['moe']['quota']    ||= 0
        
        templates['result']['visits']['maths']['byCounty'][countyId]['subCounties'][subCountyId]             ||= {}
        templates['result']['visits']['maths']['byCounty'][countyId]['subCounties'][subCountyId]['name']     ||= subCounty['label']
        templates['result']['visits']['maths']['byCounty'][countyId]['subCounties'][subCountyId]['zones']    ||= []

        subCounty['children'].map { | zoneId, zone |

          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]                   ||= {}
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['name']           ||= zone['label']
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['trips']          ||= []
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['visits']         ||= 0
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['quota']          ||= 0
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['numTeachers']    ||= 0
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['compensation']   ||= 0
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']        ||= {}
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class']          ||= {}
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1]       ||= {}
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2]       ||= {}
          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3]       ||= {}

          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['quota']  += zone['quota'].to_i
          templates['result']['visits']['national']['quota']                             += zone['quota'].to_i

          templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['numTeachers']  += zone['numTeachers'].to_i
          templates['result']['visits']['byCounty'][countyId]['numTeachers']                   += zone['numTeachers'].to_i
          templates['result']['visits']['national']['numTeachers']                             += zone['numTeachers'].to_i
      
          templates['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['zones'].push(zoneId)

          #maths
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]                   ||= {}
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['name']           ||= zone['label']
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['trips']          ||= []
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['visits']         ||= 0
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['quota']          ||= 0
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['numTeachers']    ||= 0
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['compensation']   ||= 0
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']        ||= {}
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class']          ||= {}
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1]       ||= {}
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2]       ||= {}
          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3]       ||= {}

          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['quota']  += zone['quota'].to_i
          templates['result']['visits']['maths']['national']['quota']                             += zone['quota'].to_i

          templates['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['numTeachers']  += zone['numTeachers'].to_i
          templates['result']['visits']['maths']['byCounty'][countyId]['numTeachers']                   += zone['numTeachers'].to_i
          templates['result']['visits']['maths']['national']['numTeachers']                             += zone['numTeachers'].to_i
      
          templates['result']['visits']['maths']['byCounty'][countyId]['subCounties'][subCountyId]['zones'].push(zoneId)

          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]                   ||= {}
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['name']           ||= zone['label']
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['trips']          ||= []
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['visits']         ||= 0
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['quota']          ||= 0
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['fluency']        ||= {}

          #init container for users
          templates['result']['users'][countyId]                   ||= {}
          templates['result']['users'][countyId][zoneId]           ||= {}

          #init geoJSON Containers
          templates['geoJSON']['byCounty'][countyId]         ||= {}
          templates['geoJSON']['byCounty'][countyId]['data'] ||= []

          templates['locationByZone'][zoneId]                  ||= {}
          templates['locationByZone'][zoneId]['countyId']        = countyId
          templates['locationByZone'][zoneId]['subCountyId']     = subCountyId

          zone['children'].map { | schoolId, school |
            templates['locationBySchool'][schoolId]                  ||= {}
            templates['locationBySchool'][schoolId]['countyId']        = countyId
            templates['locationBySchool'][schoolId]['subCountyId']     = subCountyId
            templates['locationBySchool'][schoolId]['zoneId']          = zoneId
          }
        }
      } 
    }
    
    return templates
  end # of processLocations

  # Process users
  def processUsers(templates)
    
    userDocs = @couch.getRequest({
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

        #duble each of these up to account for schema change over time - case-sensitive
        county   = location['County'] if !location['County'].nil?
        county   = location['county'] if !location['county'].nil?
        zone     = location['Zone'] if !location['Zone'].nil?
        zone     = location['zone'] if !location['zone'].nil?

        subCounty = templates['locationByZone'][zone]['subCountyId'] if !templates['locationByZone'][zone].nil?
        #tmpCounty = templates['locationByZone'][zone]['countyId'] if !templates['locationByZone'][zone].nil?
        #puts "#{county}-#{tmpCounty}"

        role = user['doc']['role'] || "tac-tutor"

        #verify that the user has a zone and county associated
        if !county.nil? && !zone.nil?
          username                                          = user['doc']['name']
          templates['users']['all']                       ||= {}
          templates['users'][county]                      ||= {}
          templates['users'][county][zone]                ||= {}
          templates['users'][county][zone][username]        = true

          templates['users']['all'][username]                            ||= {}
          templates['users']['all'][username]['data']                      = user['doc']
          templates['users']['all'][username]['role']                      = role

          templates['users']['all'][username]['target']                  ||= {}      # container for target zone visits
          templates['users']['all'][username]['target']['visits']        ||= 0
          templates['users']['all'][username]['target']['compensation']  ||= 0

          templates['users']['all'][username]['other']                   ||= {}      # container for non-target zone visits

          templates['users']['all'][username]['total']                   ||= {}      # container for visit and compensation totals
          templates['users']['all'][username]['total']['visits']         ||= 0       # total visits across zones
          templates['users']['all'][username]['total']['compensation']   ||= 0       # total compensation across zones
          templates['users']['all'][username]['flagged']                 ||= false   # alert to visits outside of primary zone

          # only do this if there is a valid subcounty taht currently exists
          if !subCounty.nil?

            if role == "scde"
              
              templates['result']['visits']['scde']['national']['quota']                                   += 8
              templates['result']['visits']['byCounty'][county]['scde']['quota']                           += 8
              templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['scde']['quota'] += 8

            elsif role == "esqac"

              templates['result']['visits']['esqac']['national']['quota']                                   += 10
              templates['result']['visits']['byCounty'][county]['esqac']['quota']                           += 10
              templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['esqac']['quota'] += 10

              templates['result']['visits']['priede']['national']['quota']                                   += 10
              templates['result']['visits']['byCounty'][county]['priede']['quota']                           += 10
              templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['priede']['quota'] += 10

              templates['result']['visits']['moe']['national']['quota']                                   += 10
              templates['result']['visits']['byCounty'][county]['moe']['quota']                           += 10
              templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['moe']['quota'] += 10

            end
          end
        end
      end
    }

    return templates
  end # of processUsers

#
#
#  Process each individual trip 
#
#
  # Process an individual trip
  def processTrip(trip, monthData, templates, workflows)
    #puts "Processing Trip"  

    workflowId = trip['value']['workflowId'] || trip['id']
    username   = trip['value']['user']       || ""

    # handle case of irrelevant workflow 
    return err(true, "Incomplete or Invalid Workflow: #{workflowId}") if not workflows[workflowId]
    return err(true, "Workflow does not get pre-processed: #{workflowId}") if not workflows[workflowId]['reporting']['preProcess']

    # validate user and role-workflow assocaition
    return err(true, "User does not exist: #{username}") if not templates['users']['all'][username]
    userRole = templates['users']['all'][username]['role']
    return err(true, "User role does not match with workflow: #{username} | #{templates['users']['all'][username]['role']} - targets #{workflows[workflowId]['reporting']['targetRoles']}") if not workflows[workflowId]['reporting']['targetRoles'].include? userRole

    # validate against the workflow constraints
    validated = validateTrip(trip, workflows[workflowId])
    return err(true, "Trip did not validate against workflow constraints") if not validated

    # verify school
    return err(true, "School was not found in trip") if trip['value']['school'].nil?
          
    schoolId      = trip['value']['school']
    return err(true, "School was not found in database") if templates['locationBySchool'][schoolId].nil?

    
    zoneId        = templates['locationBySchool'][schoolId]['zoneId']        || ""
    subCountyId   = templates['locationBySchool'][schoolId]['subCountyId']   || ""
    countyId      = templates['locationBySchool'][schoolId]['countyId']      || ""
    username      = trip['value']['user'].downcase
    
    #
    # Handle Role-specific calculations
    #
    if userRole == "tac-tutor" or userRole == "coach" or userRole == "CSO"

      #skip these steps if either the county or zone are no longer in the primary list 
      return err(false, "Missing County") if monthData['result']['visits']['byCounty'][countyId].nil?
      return err(false, "Missing Zones")  if monthData['result']['visits']['byCounty'][countyId]['zones'].nil?
      return err(false, "Missing Zone")   if monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId].nil?
      return err(false, "Missing Visits") if monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['visits'].nil?

      monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['id']

      return err(true, "Subject was not found in database") if trip['value']['subject'].nil?
      return err(true, "Class was not found in database") if trip['value']['class'].nil?
      #puts "trip user: #{username} - in result #{result['users']['all'][username].nil?}"
      #return if monthData['result']['users']['all'][username].nil?
      #if !trip['value']['subject'].nil? and  trip['value']['subject'] != ""and !trip['value']['class'].nil? and trip['value']['class'] != ""
        #ensure that the user exists in the db and in the result-set
        if monthData['result']['users']['all'][username].nil?
          monthData['result']['users']['all'][username]                            ||= {}
          monthData['result']['users']['all'][username]['data']                    ||= {}

          monthData['result']['users']['all'][username]['target']                  ||= {}      # container for target zone visits
          monthData['result']['users']['all'][username]['target']['visits']        ||= 0
          monthData['result']['users']['all'][username]['target']['compensation']  ||= 0

          monthData['result']['users']['all'][username]['other']                   ||= {}      # container for non-target zone visits

          monthData['result']['users']['all'][username]['total']                   ||= {}      # container for visit and compensation totals
          monthData['result']['users']['all'][username]['total']['visits']         ||= 0       # total visits across zones
          monthData['result']['users']['all'][username]['total']['compensation']   ||= 0       # total compensation across zones
          monthData['result']['users']['all'][username]['flagged']                 ||= false   # alert to visits outside of primary zone
        end

        if !monthData['result']['users'][countyId][zoneId][username].nil?
          monthData['result']['users']['all'][username]['target']['visits']  += 1

        else
          monthData['result']['users']['all'][username]['other'][countyId]                           ||= {}
          monthData['result']['users']['all'][username]['other'][countyId][zoneId]                   ||= {}
          monthData['result']['users']['all'][username]['other'][countyId][zoneId]['visits']         ||= 0
          monthData['result']['users']['all'][username]['other'][countyId][zoneId]['compensation']   ||= 0

          monthData['result']['users']['all'][username]['flagged']                                     = true
          monthData['result']['users']['all'][username]['other'][countyId][zoneId]['visits']          += 1
        end

        monthData['result']['users']['all'][username]['total']['visits']                              += 1

        #monthData['result']['visits']['national']['visits']                                           += 1
        #monthData['result']['visits']['byCounty'][countyId]['visits']                                 += 1 
        #monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['visits']                += 1
        
        #check workflowid=maths observations
        if workflowId=="62fd1403-193f-20be-7662-5589ffcfadee"
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['id']
          monthData['result']['visits']['maths']['national']['visits']                                += 1
          monthData['result']['visits']['maths']['byCounty'][countyId]['visits']                      += 1 
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['visits']     += 1
          
          #
          # Process geoJSON data for mapping
          #
          if !trip['value']['gpsData'].nil?
            point = trip['value']['gpsData']

            if !@timezone.nil?
              startDate = Time.at(trip['value']['minTime'].to_i / 1000).getlocal(@timezone)
            else 
              startDate = Time.at(trip['value']['minTime'].to_i / 1000)
            end

            point['role'] = "maths"
            point['properties'] = [
              { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
              { 'label' => 'Subject',         'value' => @subjectLegend[trip['value']['subject']] },
              { 'label' => 'Class',           'value' => trip['value']['class'] },
              { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
              { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
              { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
              { 'label' => 'TAC tutor',       'value' => titleize(trip['value']['user'].downcase) },
              { 'label' => 'Lesson Week',     'value' => trip['value']['week'] },
              { 'label' => 'Lesson Day',      'value' => trip['value']['day'] }
            ]

            monthData['geoJSON']['byCounty'][countyId]['data'].push point
          end

        elsif workflowId=="c835fc38-de99-d064-59d3-e772ccefcf7d" or workflowId=="27469912-1fa9-cac1-6810-b4e962a82b42"
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['id']
          monthData['result']['visits']['national']['visits']                                         += 1
          monthData['result']['visits']['byCounty'][countyId]['visits']                               += 1 
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['visits']              += 1
          
          #
          # Process geoJSON data for mapping
          #
          if !trip['value']['gpsData'].nil?
            point = trip['value']['gpsData']

            if !@timezone.nil?
              startDate = Time.at(trip['value']['minTime'].to_i / 1000).getlocal(@timezone)
            else 
              startDate = Time.at(trip['value']['minTime'].to_i / 1000)
            end

            point['role'] = userRole
            point['properties'] = [
              { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
              { 'label' => 'Subject',         'value' => @subjectLegend[trip['value']['subject']] },
              { 'label' => 'Class',           'value' => trip['value']['class'] },
              { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
              { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
              { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
              { 'label' => 'TAC tutor',       'value' => titleize(trip['value']['user'].downcase) },
              { 'label' => 'Lesson Week',     'value' => trip['value']['week'] },
              { 'label' => 'Lesson Day',      'value' => trip['value']['day'] }
            ]

            monthData['geoJSON']['byCounty'][countyId]['data'].push point
          end

        end
      #end
  
      #
      # process fluency data
      #
      if !trip['value']['itemsPerMinute'].nil? and
         !trip['value']['subject'].nil? and
         trip['value']['subject'] != ""and
         !trip['value']['class'].nil? and
         trip['value']['class'] != ""

        itemsPerMinute = trip['value']['itemsPerMinute']
        benchmarked    = trip['value']['benchmarked']
        met            = trip['value']['metBenchmark']

        subject = trip['value']['subject']

        #check that class 3 subject data is handled in the same way as class 1 & 2
        if subject == "english"
          subject = "english_word"
        elsif subject == "kiswahili"
          subject = "word"
        else
          subject = trip['value']['subject']
        end
        
        #puts "Subject #{subject}"

        obsClass = trip['value']['class'].to_i

        if !@reportSettings['fluency']['subjects'].include?(subject)
          @reportSettings['fluency']['subjects'].push subject
        end

        #pushUniq reportSettings['fluency']['subjects'], subject, subjectsExists

        total = 0
        itemsPerMinute.each { | ipm | 
          if !ipm.nil? 
            total += ipm 
          end
        }

        #check for maths workflow
        if workflowId=="62fd1403-193f-20be-7662-5589ffcfadee"
          monthData['result']['visits']['maths']['national']['fluency']['class']                              ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][1]                           ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][1][subject]                  ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][1][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['national']['fluency']['class'][1][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['national']['fluency']['class'][1][subject]['metBenchmark']  ||= 0
          
          monthData['result']['visits']['maths']['national']['fluency']['class'][2]                           ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][2][subject]                  ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][2][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['national']['fluency']['class'][2][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['national']['fluency']['class'][2][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['maths']['national']['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['maths']['national']['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['maths']['national']['fluency']['class'][obsClass][subject]['metBenchmark']  += met

          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class']                              ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][1]                           ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][1][subject]                  ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][1][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][1][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][1][subject]['metBenchmark']  ||= 0
          
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][2]                           ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][2][subject]                  ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][2][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][2][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][2][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][obsClass][subject]['metBenchmark']  += met

          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class']                              ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1]                           ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]                  ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]['metBenchmark']  ||= 0
          
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2]                           ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]                  ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['metBenchmark']  += met

        elsif workflowId=="c835fc38-de99-d064-59d3-e772ccefcf7d" or workflowId=="27469912-1fa9-cac1-6810-b4e962a82b42"
          monthData['result']['visits']['national']['fluency']['class']                              ||= {}
          monthData['result']['visits']['national']['fluency']['class'][1]                           ||= {}
          monthData['result']['visits']['national']['fluency']['class'][1][subject]                  ||= {}
          monthData['result']['visits']['national']['fluency']['class'][1][subject]['sum']           ||= 0
          monthData['result']['visits']['national']['fluency']['class'][1][subject]['size']          ||= 0
          monthData['result']['visits']['national']['fluency']['class'][1][subject]['metBenchmark']  ||= 0
          
          monthData['result']['visits']['national']['fluency']['class'][2]                           ||= {}
          monthData['result']['visits']['national']['fluency']['class'][2][subject]                  ||= {}
          monthData['result']['visits']['national']['fluency']['class'][2][subject]['sum']           ||= 0
          monthData['result']['visits']['national']['fluency']['class'][2][subject]['size']          ||= 0
          monthData['result']['visits']['national']['fluency']['class'][2][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['national']['fluency']['class'][3]                           ||= {}
          monthData['result']['visits']['national']['fluency']['class'][3][subject]                  ||= {}
          monthData['result']['visits']['national']['fluency']['class'][3][subject]['sum']           ||= 0
          monthData['result']['visits']['national']['fluency']['class'][3][subject]['size']          ||= 0
          monthData['result']['visits']['national']['fluency']['class'][3][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['national']['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['national']['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['national']['fluency']['class'][obsClass][subject]['metBenchmark']  += met

          monthData['result']['visits']['byCounty'][countyId]['fluency']['class']                              ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][1]                           ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][1][subject]                  ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][1][subject]['sum']           ||= 0
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][1][subject]['size']          ||= 0
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][1][subject]['metBenchmark']  ||= 0
          
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][2]                           ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][2][subject]                  ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][2][subject]['sum']           ||= 0
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][2][subject]['size']          ||= 0
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][2][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][3]                           ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][3][subject]                  ||= {}
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][3][subject]['sum']           ||= 0
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][3][subject]['size']          ||= 0
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][3][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['byCounty'][countyId]['fluency']['class'][obsClass][subject]['metBenchmark']  += met

          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class']                              ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1]                           ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]                  ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]['sum']           ||= 0
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]['size']          ||= 0
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1][subject]['metBenchmark']  ||= 0
          
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2]                           ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]                  ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]['sum']           ||= 0
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]['size']          ||= 0
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3]                           ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]                  ||= {}
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]['sum']           ||= 0
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]['size']          ||= 0
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['metBenchmark']  += met

        end

      end


    elsif userRole == "scde"

      return err(false, "SCDE: Missing County") if monthData['result']['visits']['byCounty'][countyId].nil?
      return err(true, "SCDE: Missing Sub County")  if monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId].nil?

      monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['trips'].push trip['id']
      
      monthData['result']['visits']['scde']['national']['visits']                                       += 1
      monthData['result']['visits']['byCounty'][countyId]['scde']['visits']                             += 1
      monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['visits'] += 1

      #
      # Process geoJSON data for mapping
      #
      if !trip['value']['gpsData'].nil?
        point = trip['value']['gpsData']

        if !@timezone.nil?
          startDate = Time.at(trip['value']['minTime'].to_i / 1000).getlocal(@timezone)
        else 
          startDate = Time.at(trip['value']['minTime'].to_i / 1000)
        end

        point['role'] = userRole
        point['properties'] = [
          { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
          { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
          { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
          { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
          { 'label' => 'SCDE',            'value' => titleize(trip['value']['user'].downcase) }
        ]

        monthData['geoJSON']['byCounty'][countyId]['data'].push point
      end

    elsif userRole == "esqac" or userRole == "ESQAC"
      puts "** processing ESQAC Trip"

      #skip these steps if either the county or zone are no longer in the primary list 
      return err(false, "ESQAC: Missing County") if monthData['result']['visits']['byCounty'][countyId].nil?
      return err(true, "ESQAC: Missing Sub County")  if monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId].nil?
      #return err(true, "ESQAC: Missing Zone")   if monthData['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId].nil?
      #return err(true, "ESQAC: Missing Visits") if monthData['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['visits'].nil?

      #check for the differentiator flags
      #1.Tusome/Priede Flag
      #2. Subject

      t = trip['value']

      puts "Trip data #{t}"

      if !trip['value']['subject'].nil? and !trip['value']['class'].nil?
        #priede or tusome
        if trip['value']['subject'] == "operation"
          #priede
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['trips'].push trip['id']
      
          monthData['result']['visits']['priede']['national']['visits']                                       += 1
          monthData['result']['visits']['byCounty'][countyId]['priede']['visits']                             += 1
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['visits'] += 1
        else
          #tusome
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['trips'].push trip['id']
      
          monthData['result']['visits']['esqac']['national']['visits']                                       += 1
          monthData['result']['visits']['byCounty'][countyId]['esqac']['visits']                             += 1
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['visits'] += 1
        end
      end

      
      
      #
      # Process geoJSON data for mapping
      #
      if !trip['value']['gpsData'].nil?
        point = trip['value']['gpsData']

        if !@timezone.nil?
          startDate = Time.at(trip['value']['minTime'].to_i / 1000).getlocal(@timezone)
        else 
          startDate = Time.at(trip['value']['minTime'].to_i / 1000)
        end

        point['role'] = userRole
        point['properties'] = [
          { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
          { 'label' => 'Subject',         'value' => @subjectLegend[trip['value']['subject']] },
          { 'label' => 'Class',           'value' => trip['value']['class'] },
          { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
          { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
          { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
          { 'label' => 'ESQAC',           'value' => titleize(trip['value']['user'].downcase) },
          { 'label' => 'Lesson Week',     'value' => trip['value']['week'] },
          { 'label' => 'Lesson Day',      'value' => trip['value']['day'] }
        ]

        monthData['geoJSON']['byCounty'][countyId]['data'].push point
      end

    else 
      return err(false, "Not handling these roles yet: #{userRole}")
    end

  end # of processTrip

#
#
#  Post-Process the trip data
#
#
  def postProcessTrips(monthData, templates)
    puts "Post-Processing Trips"

    templates['users']['all'].map{ | userName, user |
      role = user['role']
      if templates['users']['all'][userName]['data']['location'].nil?
            puts "Error: Cannot find location information for username: #{userName}"
          end

          #next if templates['users']['all'][userName]['data']['location'].nil?

          location = templates['users']['all'][userName]['data']['location']
          
          countyId  = location['county'] if !location['county'].nil?
          zoneId    = location['zone'] if !location['zone'].nil?
          
          countyName  = location['County'] if !location['County'].nil?
          #puts "processing county #{countyName}"
          #ensure user has visit data
          if !monthData['result']['users']['all'][userName].nil? && !monthData['result']['users']['all'][userName]['other'][countyId].nil?
             #loop users visit data for assigned county and compute compensation
             monthData['result']['users']['all'][userName]['other'][countyId].map{ | visitZoneId, zoneData |
                #compute rate for zone
                puts "Processing compensation for #{userName}"
                visits = zoneData['visits']
                teachers =  templates['result']['visits']['byCounty'][countyId]['zones'][visitZoneId]['numTeachers']
                #teachers should be less than or equal 40
                if teachers>40
                  teachers = 40
                end  

                compensation = 0
                #tac tutor
                if role == 'tac-tutor' or role == 'cso'
                  
                  completePct = (visits + 0.0) / teachers
                                    
                  #completePct should be less that or equal to 1
          
                  #compute compensation for primary zone - additional Ksh 500 
                  if visitZoneId == zoneId
                      compensation  = (((completePct > 1) ? 1 : completePct) * 10000 + 500).round(2)
                      #puts "User: #{userName} Vists: #{visits} Quota: #{quota} Rate: #{completePct} Reimbursement PZ: #{compensation}"
                  else 
                      #compute compensation for secondary zones - less Ksh 500
                      compensation = (((completePct > 1) ? 1 : completePct) * 10000).round(2)
                      #puts "User: #{userName} Vists: #{visits} Quota: #{quota} Rate: #{completePct} Reimbursement SZ: #{compensation}"
                  end
                elsif role == 'coach' 
                  #coach
                  completePct = (visits + 0.0) / (teachers * 2)
                                    
                  #completePct should be less that or equal to 1
                  compensation = (((completePct > 1) ? 1 : completePct) * 6000).round(2)

                  #puts "processing user #{userName} role #{role} Quota: #{quota} Rate: #{completePct} amount #{compensation}"
                end

                
                #save compensation data
                zoneData["compensation"] += compensation
                monthData['result']['users']['all'][userName]['total']['compensation'] += compensation
                monthData['result']['users']['all'][userName]['data']['Mpesa']         = user['data']['phone']

                monthData['result']['visits']['byCounty'][countyId]['compensation']   += compensation
                monthData['result']['visits']['national']['compensation']             += compensation

                monthData['result']['visits']['byCounty'][countyId]['zones'][visitZoneId]['compensation']   += compensation
             }
          end
        
    }

  end

#
#
#  Validate the trip results against the constraints stored in the workflow 
#
#
  def validateTrip(trip, workflow)
    # return valid if validation not enabled for trip
    return true if not workflow['observationValidation']
    return true if not workflow['observationValidation']['enabled']
    return true if not workflow['observationValidation']['constraints']

    #assume incomplete if there is no min and max time defined
    return false if not trip['value']['minTime']
    return false if not trip['value']['maxTime']

    if !@timezone.nil?
      startDate = Time.at(trip['value']['minTime'].to_i / 1000).getlocal(@timezone)
      endDate   = Time.at(trip['value']['maxTime'].to_i / 1000).getlocal(@timezone)
    else 
      startDate = Time.at(trip['value']['minTime'].to_i / 1000)
      endDate   = Time.at(trip['value']['maxTime'].to_i / 1000)
    end

    workflow['observationValidation']['constraints'].each { | type, constraint |
      if type == "timeOfDay"
        startRange = constraint['startTime']['hour']
        endRange   = constraint['endTime']['hour']
        return false if not startDate.hour.between?(startRange, endRange)

      elsif type == "duration"
        if constraint["hours"]
          return false if TimeDifference.between(startDate, endDate).in_hours < constraint["hours"]
        elsif constraint["minutes"]
          return false if TimeDifference.between(startDate, endDate).in_minutes < constraint["minutes"]
        elsif constraint["seconds"]
          return false if TimeDifference.between(startDate, endDate).in_seconds < constraint["seconds"]
        end
      end 
    }

    return true
  end

  def err(display, msg)
    @tripsSkipped = @tripsSkipped + 1
    puts "Trip Skipped: #{msg}" if display
  end

  def getSkippedCount()
    return @tripsSkipped
  end

  def resetSkippedCount()
    @tripsSkipped = 0
  end

end
