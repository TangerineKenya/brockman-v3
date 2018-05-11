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
    @subjectLegend   = { "english_word" => "English", "kiswahili_word" => "Kiswahili", "operation" => "Maths" } 

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

    #SNE observations
    templates['result']['visits']['sne']                              ||= {}
    templates['result']['visits']['sne']['byCounty']                  ||= {}
    templates['result']['visits']['sne']['national']                  ||= {}
    templates['result']['visits']['sne']['national']['visits']        ||= 0
    templates['result']['visits']['sne']['national']['quota']         ||= 0
    templates['result']['visits']['sne']['national']['numTeachers']   ||= 0
    templates['result']['visits']['sne']['national']['compensation']  ||= 0
    templates['result']['visits']['sne']['national']['fluency']       ||= {}

    templates['result']['users']           ||= {}  #stores list of all users and zone associations
    templates['result']['users']['all']    ||= {}  #stores list of all users

    templates['result']['compensation']               ||= {}
    templates['result']['compensation']['byCounty']   ||= {}
    templates['result']['compensation']['national']   ||= 0

    # define scope or the geoJSON files
    templates['geoJSON']               ||= {}
    templates['geoJSON']['byCounty']   ||= {}

    #rti staff data
    templates['result']['staff']                               ||= {}
    templates['result']['staff']['byCounty']                   ||= {}

    templates['result']['staff']['users']                      ||= {}
    #templates['result']['staff']['users']['all']               ||= {}

    templates['result']['staff']['national']                   ||= {}
    templates['result']['staff']['national']['visits']         ||= 0
    templates['result']['staff']['national']['gpsvisits']      ||= 0
    templates['result']['staff']['national']['quota']          ||= 0

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

      #sne
      templates['result']['visits']['sne']['byCounty'][countyId]                              ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['name']                      ||= county['label']
      templates['result']['visits']['sne']['byCounty'][countyId]['subCounties']               ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['zones']                     ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['visits']                    ||= 0
      templates['result']['visits']['sne']['byCounty'][countyId]['quota']                     ||= 0
      templates['result']['visits']['sne']['byCounty'][countyId]['numTeachers']               ||= 0
      templates['result']['visits']['sne']['byCounty'][countyId]['compensation']              ||= 0
      templates['result']['visits']['sne']['byCounty'][countyId]['fluency']                   ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['fluency']['class']          ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['fluency']['class'][1]       ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['fluency']['class'][2]       ||= {}
      templates['result']['visits']['sne']['byCounty'][countyId]['fluency']['class'][3]       ||= {}

      templates['result']['visits']['maths']['byCounty'][countyId]['quota']    = county['quota']

      #staff data
      templates['result']['staff']['byCounty'][countyId]                  ||= {}
      templates['result']['staff']['byCounty'][countyId]['name']          ||= county['label']
      templates['result']['staff']['byCounty'][countyId]['visits']        ||= 0
      templates['result']['staff']['byCounty'][countyId]['gpsvisits']     ||= 0
      templates['result']['staff']['byCounty'][countyId]['quota']         ||= 0
      templates['result']['staff']['byCounty'][countyId]['zones']         ||= {}
      templates['result']['staff']['byCounty'][countyId]['users']         ||= {}
      templates['result']['staff']['byCounty'][countyId]['schools']       ||= {}

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

          #sne
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]                   ||= {}
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['name']           ||= zone['label']
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['trips']          ||= []
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['visits']         ||= 0
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['quota']          ||= 0
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['numTeachers']    ||= 0
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['compensation']   ||= 0
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['fluency']        ||= {}
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['fluency']['class']          ||= {}
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][1]       ||= {}
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][2]       ||= {}
          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3]       ||= {}

          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['quota']  += zone['quota'].to_i
          templates['result']['visits']['sne']['national']['quota']                             += zone['quota'].to_i

          templates['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['numTeachers']  += zone['numTeachers'].to_i
          templates['result']['visits']['sne']['byCounty'][countyId]['numTeachers']                   += zone['numTeachers'].to_i
          templates['result']['visits']['sne']['national']['numTeachers']                             += zone['numTeachers'].to_i
      
          #templates['result']['visits']['sne']['byCounty'][countyId]['subCounties'][subCountyId]['zones'].push(zoneId)

          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]                   ||= {}
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['name']           ||= zone['label']
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['trips']          ||= []
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['visits']         ||= 0
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['quota']          ||= 0
          # templates['result']['visits']['esqac']['byCounty'][countyId]['zones'][zoneId]['fluency']        ||= {}

          #staff data count zones in the county
          templates['result']['staff']['byCounty'][countyId]['quota']                           += 1
          templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]                   ||= {}
          templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['name']           ||= zone['label']
          templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['visits']         ||= 0
          templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['gpsvisits']      ||= 0
          templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['quota']          ||= 0
          templates['result']['staff']['national']['quota']                                      += 1
          templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools']        ||= {}
          
          #templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['quota']          += 1

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

            #staff data
            templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]                ||= {}
            templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['name']        ||= school['label']
            templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['zone']        ||= zone['label']
            templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['zoneId']      ||= zoneId
            templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['visits']      ||= 0
            templates['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['gpsvisits']   ||= 0

            #track visits to schools
            templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools']                          ||= {}
            templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]                ||= {}
            templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['name']        ||= school['label']
            #templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['zone']        ||= zone['label']
            #templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['zoneId']      ||= zoneId
            templates['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['visits']      ||= 0
          }
        }
      } 
    }
    
    return templates
  end # of processLocations

  # Process users
  def processUsers(templates)
    userDocs = @couch.postRequest({
      :view => "users",
      :params => { 
        "reduce" => false,
        "include_docs" => true
      },
      :data => {"keys" => ["user-profile"]},
      :parseJson => true
    })

    puts " #{userDocs['rows'].size} Total Users"
    
    #associate users with their county and zone for future processing
    if !userDocs['rows'].nil?
      userDocs['rows'].map{ | user | 
        if !user['value'].nil?
          username                                          = user['value']['_id']
          #check user has items
          return err(true, "user #{username} has no values") if user['value']['items'].nil?
          userDoc                                              = user['value']['items']

          templates['users']['all']                       ||= {}
          #templates['users'][county]                      ||= {}
          #templates['users'][county][zone]                ||= {}
          #templates['users'][county][zone][username]        = true

          templates['users']['all'][username]                            ||= {}
          #templates['users']['all'][username]['name']                      = ''
          #templates['users']['all'][username]['role']                      = role

          templates['users']['all'][username]['target']                  ||= {}      # container for target zone visits
          templates['users']['all'][username]['target']['visits']        ||= 0
          templates['users']['all'][username]['target']['compensation']  ||= 0

          templates['users']['all'][username]['other']                   ||= {}      # container for non-target zone visits

          templates['users']['all'][username]['total']                   ||= {}      # container for visit and compensation totals
          templates['users']['all'][username]['total']['visits']         ||= 0       # total visits across zones
          templates['users']['all'][username]['total']['compensation']   ||= 0       # total compensation across zones
          templates['users']['all'][username]['flagged']                 ||= false   # alert to visits outside of primary zone

          if !user['value']['items'].nil?
            userDoc.map { | items |  
             #puts "#{item['inputs']}"
             if !items['inputs'].nil?
                items['inputs'].map { |item|  
                  
                  templates['users']['all'][username]['data']                      = item

                  return err(true, "user #{username} has no values") if item.nil?
                  role = 'cso'
                   #get role values
                  if item['name'] == 'role'
                     roles = item['value']

                     roles.map { | e |  
                       if e['value'] == 'on'
                         #puts "role #{e['name']}"
                        role = e['name']
                        templates['users']['all'][username]['role']                      = e['name']
                       end
                     }
                  end
                  first_name = ''
                  if item['name'] == 'first_name'
                    first_name = item['value']
                  end
                  last_name = ''
                  if item['name'] == 'last_name'
                    last_name = item['value']
                  end
                  templates['users']['all'][username]['name']                      ||= first_name+' '+last_name

                  #puts "Names: #{templates['users']['all'][username]['name']}"

                  #get location values
                  county = ''
                  subCounty = ''
                  zone = ''

                  if item['name'] == 'location'
                    locations = item['value']
                    locations.map { | location |  
                      if location['level'] == 'county'
                        county = location['value']
                      end
                      if location['level'] == 'subcounty'
                        subCounty = location['value']
                      end
                      if location['level'] == 'zone'
                        zone = location['value']
                      end
                    }
                  end

                  if !county.nil? && !zone.nil?
                    templates['users'][county]                      ||= {}
                    templates['users'][county][zone]                ||= {}
                    templates['users'][county][zone][username]        = true
                  end 

                  #staff users
                  if role == 'rti-staff'
                    templates['result']['staff']['users'][username]                    ||= {}
                    #templates['result']['staff']['users'][username][county]            ||= {}
                    #templates['result']['staff']['users'][username][county][zone]      ||= {}
                    templates['result']['staff']['users'][username]['role']              = role
                    templates['result']['staff']['users'][username]['data']            ||= item
                  end
                  
                  # only do this if there is a valid subcounty that currently exists
                  if !county.nil? && !subCounty.nil?

                    if role == "scde"
                      templates['result']['visits']['scde']['national']['quota']                                   += 8
                      #templates['result']['visits']['byCounty'][county]['scde']['quota']                           += 8
                      #templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['scde']['quota'] += 8

                    elsif role == "esqac"

                      templates['result']['visits']['esqac']['national']['quota']                                   += 10
                      templates['result']['visits']['byCounty'][county]['esqac']['quota']                           += 10
                      #templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['esqac']['quota'] += 10

                      templates['result']['visits']['priede']['national']['quota']                                   += 10
                      templates['result']['visits']['byCounty'][county]['priede']['quota']                           += 10
                      #templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['priede']['quota'] += 10

                      templates['result']['visits']['moe']['national']['quota']                                   += 10
                      templates['result']['visits']['byCounty'][county]['moe']['quota']                           += 10
                      #templates['result']['visits']['byCounty'][county]['subCounties'][subCounty]['moe']['quota'] += 10

                    end
                  end
                }
             end
            } 
          end
        end
      }    
    end

    return templates
  end # of processUsers

#
#
#  Process each individual trip 
#
#
  # Process an individual trip
  def processTrip(trip, monthData, templates, workflows)
    puts "Processing Trip"  
    workflowId = trip['value']['form']['id']    
    username   = ''
    schoolId   = ''
    grade      = ''
    subject    = ''
    gpsData    = {}
    #set value for gpsData
    gpsData['type']                      = 'Feature'
    gpsData['properties']              ||= []
    gpsData['geometry']                ||= {}
    gpsData['geometry']['type']          = 'Point'
    gpsData['geometry']['coordinates'] ||= []

    if !trip['value']['items'].nil?
      #puts "workflow #{trip['value']['items']}"
      results = trip['value']['items']
      #loop on result items
      results.map { | items |   
        if !items['inputs'].nil?
          items['inputs'].map { |item|  
            
            if item['name'] == 'userProfileId'
              username   = item['value']  || ""
            end

            if item['name'] == 'location'
              locations = item['value']
              locations.map { | location |  
                if location['level'] == 'school'
                  schoolId = location['value'] || ""
                end
              }
            end
            #get subject
            if item['name'] == 'subject'
              subjects = item['value'] || ""
              subjects.map { | e |  
                #puts "Subject: #{e}"
                #if location['level'] == 'school'
                #  schoolId = location['value'] || ""
                #end
                if e['value'] == 'on'
                  subject = e['name']
                end

              }
            end
            #get class / grade
            if item['name'] == 'class' or item['name'] == 'grade'
              grades = item['value'] || ""
              grades.map { | e |  
                #puts "Grade: #{e}"
                #if location['level'] == 'school'
                #  schoolId = location['value'] || ""
                #end
                if e['value'] == 'on'
                  grade = e['name'] 
                end
              }
            end
            if item['name'] == 'gps-coordinates'
              #puts "#{item['value']['longitude']}, #{item['value']['latitude']}"
              gpsData['geometry']['coordinates'] = [item['value']['longitude'], item['value']['latitude']]
            end
          }          
        end
      }
    end

    #check for completeness
    return err(true, "Incomplete trip") if not trip['value']['form']['complete'] == true

    # validate user and role-workflow assocaition
    return err(true, "User does not exist: #{username}") if not templates['users']['all'][username]
    userRole = templates['users']['all'][username]['role']
    #users full names
    user = templates['users']['all'][username]['name'] 

    return err(true, "User role does not match with workflow: #{username} | #{templates['users']['all'][username]['role']} - targets #{workflows[workflowId]['targetRoles']}") if not workflows[workflowId]['targetRoles'].include? userRole

    # validate against the workflow constraints
    validated = validateTrip(trip, workflows[workflowId])
    #return err(true, "Trip did not validate against workflow constraints") if not validated

    return err(true, "School was not found in database") if templates['locationBySchool'][schoolId].nil?

    zoneId        = templates['locationBySchool'][schoolId]['zoneId']        || ""
    subCountyId   = templates['locationBySchool'][schoolId]['subCountyId']   || ""
    countyId      = templates['locationBySchool'][schoolId]['countyId']      || ""
    
    #grade 3 instruments don't have class selection - pass value for the grade
    #grade 3 forms don't have grade selection in the input. Default to grade 3 to prevent nil errors
    if workflowId == "maths-grade3" or workflowId == "Gradethreeobservationtool"
      grade = 3
    end

    #handle subject the same way across all forms
    if subject == "word"
      subject = "kiswahili_word"
    end

    #
    # Handle Role-specific calculations
    #

    if workflowId == "tusome-classroom-observation-tool-for-sne"
          monthData['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['_id']
          monthData['result']['visits']['sne']['national']['visits']                                += 1
          monthData['result']['visits']['sne']['byCounty'][countyId]['visits']                      += 1 
          monthData['result']['visits']['sne']['byCounty'][countyId]['zones'][zoneId]['visits']     += 1
    end 

    if userRole == "coach" or userRole == "cso"
      
      #skip these steps if either the county or zone are no longer in the primary list 
      return err(false, "Missing County") if monthData['result']['visits']['byCounty'][countyId].nil?
      return err(false, "Missing Zones")  if monthData['result']['visits']['byCounty'][countyId]['zones'].nil?
      return err(false, "Missing Zone")   if monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId].nil?
      return err(false, "Missing Visits") if monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['visits'].nil?

      monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['_id']

      return err(true, "Subject was not found in database") if subject.nil?
      return err(true, "Class was not found in database") if grade.nil?

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

      #process each workflow 
      if workflowId == "maths-teachers-observation-tool" or workflowId == "maths-grade3" #priede data
        monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['_id']
        monthData['result']['visits']['maths']['national']['visits']                                += 1
        monthData['result']['visits']['maths']['byCounty'][countyId]['visits']                      += 1 
        monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['visits']     += 1
          
        #
        # Process geoJSON data for mapping
        #
        if !gpsData.nil?
            point = gpsData

            if !@timezone.nil?
              startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000).getlocal(@timezone)
            else 
              startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000)
            end

            point['role'] = "maths"
            point['properties'] = [
              { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
              { 'label' => 'Subject',         'value' => @subjectLegend[subject] },
              { 'label' => 'Class',           'value' => grade.to_i },
              { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
              { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
              { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
              { 'label' => 'CSO',             'value' => titleize(user.downcase) },
              { 'label' => 'Lesson Week',     'value' => '' },
              { 'label' => 'Lesson Day',      'value' => '' }
            ]
            
            monthData['geoJSON']['byCounty'][countyId]['data'].push point
        end
      elsif workflowId == "class-12-lesson-observation-with-pupil-books" or workflowId == "Gradethreeobservationtool" #tusome data
        monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['trips'].push trip['_id']
        monthData['result']['visits']['national']['visits']                                         += 1
        monthData['result']['visits']['byCounty'][countyId]['visits']                               += 1 
        monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['visits']              += 1
          
        #school visits
        monthData['result']['visits']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['visits']   += 1

        #
        # Process geoJSON data for mapping
        # 
        if !gpsData.nil?
            point = gpsData

            if !@timezone.nil?
              startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000).getlocal(@timezone)
            else 
              startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000)
            end

            point['role'] = userRole
            point['properties'] = [
              { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
              { 'label' => 'Subject',         'value' => @subjectLegend[subject] },
              { 'label' => 'Class',           'value' => grade.to_i },
              { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
              { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
              { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
              { 'label' => 'CSO',             'value' => titleize(user.downcase) },
              { 'label' => 'Lesson Week',     'value' => '' },
              { 'label' => 'Lesson Day',      'value' => '' }
            ]
            #puts "Gps: #{point}"
            #puts "Grade: #{grade}"
            monthData['geoJSON']['byCounty'][countyId]['data'].push point
          end
      end

  
      #
      # process fluency data
      #
      fluency = fluencyRates(trip, grade, subject)

      if !fluency.nil? and 
         !subject.nil? and
         subject !=  '' and
         !grade.nil? and
         grade != ' '
        
        if workflowId == "maths-teachers-observation-tool" or workflowId == "maths-grade3"
          #itemsPerMinute = fluency['itemsPerMinute']
          benchmarked    = fluency['benchmarked']
          met            = fluency['metBenchmark']

          total = fluency['itemsPerMinute']

          obsClass = grade.to_i

          if !@reportSettings['fluency']['subjects'].include?(subject)
            @reportSettings['fluency']['subjects'].push subject
          end

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

          monthData['result']['visits']['maths']['national']['fluency']['class'][3]                           ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][3][subject]                  ||= {}
          monthData['result']['visits']['maths']['national']['fluency']['class'][3][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['national']['fluency']['class'][3][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['national']['fluency']['class'][3][subject]['metBenchmark']  ||= 0

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

          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][3]                           ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][3][subject]                  ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][3][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][3][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['fluency']['class'][3][subject]['metBenchmark']  ||= 0

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

          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3]                           ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]                  ||= {}
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]['sum']           ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]['size']          ||= 0
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][3][subject]['metBenchmark']  ||= 0

          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['sum']           += total
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['size']          += benchmarked
          monthData['result']['visits']['maths']['byCounty'][countyId]['zones'][zoneId]['fluency']['class'][obsClass][subject]['metBenchmark']  += met

        elsif workflowId == "class-12-lesson-observation-with-pupil-books" or workflowId == "Gradethreeobservationtool"
          
          #itemsPerMinute = trip['value']['itemsPerMinute']
          benchmarked    = fluency['benchmarked']
          met            = fluency['metBenchmark']

          total = fluency['itemsPerMinute']

          obsClass = grade.to_i

          if !@reportSettings['fluency']['subjects'].include?(subject)
            @reportSettings['fluency']['subjects'].push subject
          end
          
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

      monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['trips'].push trip['_id']
      
      monthData['result']['visits']['scde']['national']['visits']                                       += 1
      monthData['result']['visits']['byCounty'][countyId]['scde']['visits']                             += 1
      monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['scde']['visits'] += 1

      #
      # Process geoJSON data for mapping
      #
      if !gpsData.nil?
        point = gpsData

        if !@timezone.nil?
          startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000).getlocal(@timezone)
        else 
          startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000)
        end

        point['role'] = userRole
        point['properties'] = [
          { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
          { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
          { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
          { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
          { 'label' => 'SCDE',            'value' => titleize(user.downcase) }
        ]

        monthData['geoJSON']['byCounty'][countyId]['data'].push point
      end

    elsif userRole == "esqac"
      return err(false, "ESQAC: Missing County") if monthData['result']['visits']['byCounty'][countyId].nil?
      return err(true, "ESQAC: Missing Sub County")  if monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId].nil?

      if !subject.nil? and !grade.nil?
        #priede or tusome
        if subject == "operation"
          #priede
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['trips'].push trip['_id']
      
          monthData['result']['visits']['priede']['national']['visits']                                       += 1
          monthData['result']['visits']['byCounty'][countyId]['priede']['visits']                             += 1
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['priede']['visits'] += 1
        else
          #tusome
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['trips'].push trip['_id']
      
          monthData['result']['visits']['esqac']['national']['visits']                                       += 1
          monthData['result']['visits']['byCounty'][countyId]['esqac']['visits']                             += 1
          monthData['result']['visits']['byCounty'][countyId]['subCounties'][subCountyId]['esqac']['visits'] += 1
        end
      end

      #
      # Process geoJSON data for mapping
      #
      if !gpsData.nil?
        point = gpsData

        if !@timezone.nil?
          startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000).getlocal(@timezone)
        else 
          startDate = Time.at(trip['value']['startUnixtime'].to_i / 1000)
        end

        point['role'] = userRole
        point['properties'] = [
          { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
          { 'label' => 'Subject',         'value' => @subjectLegend[subject] },
          { 'label' => 'Class',           'value' => grade.to_i},
          { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
          { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
          { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
          { 'label' => 'ESQAC',           'value' => titleize(user.downcase) },
          { 'label' => 'Lesson Week',     'value' => '' },
          { 'label' => 'Lesson Day',      'value' => ''}
        ]

        monthData['geoJSON']['byCounty'][countyId]['data'].push point
      end

    else 
      return err(false, "Not handling these roles yet: #{userRole}")
    end

  end # of processTrip

  #
  #
  #process trip information for staff
  #
  #
  def processStaffTrip(trip, monthData, templates, workflows)

    workflowId = trip['value']['workflowId'] || trip['id']
    username   = trip['value']['user']       || ""

    # handle case of irrelevant workflow 
    return err(true, "Incomplete or Invalid Workflow: #{workflowId}") if not workflows[workflowId]
    return err(true, "Workflow does not get pre-processed: #{workflowId}") if not workflows[workflowId]

    # validate user and role-workflow assocaition
    return err(true, "User does not exist: #{username}") if not templates['users']['all'][username]
    userRole = templates['users']['all'][username]['role']
    return err(true, "User role does not match with workflow: #{username} | #{templates['users']['all'][username]['role']} - targets #{workflows[workflowId]['targetRoles']}") if not workflows[workflowId]['targetRoles'].include? userRole

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

    #check workflow id is rti tool
    if workflowId == "1d67fd61-fb6b-fa4a-c4a1-0d2f1af421da" and userRole == "rti-staff"
      
      subject = trip

      puts "** Start Processing Staff Trip"
      
      monthData['result']['staff']['byCounty'][countyId]['users'][username]                     ||= {}
      monthData['result']['staff']['byCounty'][countyId]['users'][username]['visits']           ||= 0
      monthData['result']['staff']['byCounty'][countyId]['users'][username]['gpsvisits']        ||= 0
      monthData['result']['staff']['users'][username]['total']                                  ||= 0
      monthData['result']['staff']['users'][username]['visits']                                 ||= {}
      monthData['result']['staff']['users'][username]['visits'][countyId]                       ||= {}
      monthData['result']['staff']['users'][username]['visits'][countyId]['visits']             ||= 0
      monthData['result']['staff']['users'][username]['visits'][countyId]['gpsvisits']          ||= 0
      monthData['result']['staff']['users'][username]['visits'][countyId]['quota']              ||= 0

      
      monthData['result']['staff']['users'][username]['total']                                   += 1
      
      monthData['result']['staff']['users'][username]['visits'][countyId]['name']                 = templates['result']['staff']['byCounty'][countyId]['name'] 
      
      monthData['result']['staff']['users'][username]['visits'][countyId]['quota']                = templates['result']['staff']['byCounty'][countyId]['quota']
      
      #separate trips with gps and those without
      if !trip['value']['gpsData'].nil?
        monthData['result']['staff']['byCounty'][countyId]['gpsvisits']                            += 1 
        monthData['result']['staff']['byCounty'][countyId]['zones'][zoneId]['gpsvisits']           += 1
      
        monthData['result']['staff']['byCounty'][countyId]['users'][username]['gpsvisits']         += 1
        monthData['result']['staff']['users'][username]['visits'][countyId]['gpsvisits']           += 1

        monthData['result']['staff']['national']['gpsvisits']                                      += 1

        monthData['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['gpsvisits'] += 1

      else

        monthData['result']['staff']['byCounty'][countyId]['visits']                               += 1 
        monthData['result']['staff']['byCounty'][countyId]['zones'][zoneId]['visits']              += 1
      
        monthData['result']['staff']['byCounty'][countyId]['users'][username]['visits']            += 1
        monthData['result']['staff']['users'][username]['visits'][countyId]['visits']              += 1

        monthData['result']['staff']['national']['visits']                                         += 1

        monthData['result']['staff']['byCounty'][countyId]['zones'][zoneId]['schools'][schoolId]['visits'] += 1
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

            point['role'] = "staff"
            point['properties'] = [
              { 'label' => 'Date',            'value' => startDate.strftime("%d-%m-%Y %H:%M") },
              { 'label' => 'County',          'value' => titleize(@locationList['locations'][countyId]['label'].downcase) },
              { 'label' => 'Zone',            'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['label'].downcase) },
              { 'label' => 'School',          'value' => titleize(@locationList['locations'][countyId]['children'][subCountyId]['children'][zoneId]['children'][schoolId]['label'].downcase) },
              { 'label' => 'Staff',           'value' => titleize(trip['value']['user'].downcase) }
            ]

            monthData['geoJSON']['byCounty'][countyId]['data'].push point
          end
    end

  end #end processStaffTrips

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
  # For each grid test - Count the correct words read, 
  # the number of leaners assessed which is equal to 1 and 
  # if the benchmark for the subject and grade have been met
  #

  def itemsPerMinute(item, grade, subject)
    correctItems   = 0
    itemsPerMinute = 0
    fluency                   ||= {}
    fluency['itemsPerMinute'] ||= 0
    fluency['metBenchmark']   ||= 0
    fluency['benchmarked']    ||= 0

    if !item['value'].nil?
      #check that grid test has been completed
      #if item['incomplete'] == false
        #get total items
        totalItems = item['value'].length

        totalTime = item['duration']
        timeLeft  = item['timeRemaining']
          
        item['value'].map { | e |  
          if e['pressed'] == false
            correctItems  += 1
          end
        }

        if ((totalTime - timeLeft) / totalTime)  > 0
          itemsPerMinute = (totalItems - (totalItems - correctItems)) / ((totalTime - timeLeft) / totalTime)  
        end
        
        ipm = itemsPerMinute.to_i

        #ignore and exit function where ipm is greater than 120
        if Integer(ipm) >=120
          return
        end

        #for each grid test pass out neccesasary values
        fluency['itemsPerMinute'] = ipm
                
        obsClass = grade.to_i
        #(30..120) === 
        if Integer(ipm) >= 30  and 
          subject == "english_word" and 
          obsClass.eql?(1)
            fluency['metBenchmark'] += 1
        end
        #>=65 #(65..120) ===
        if Integer(ipm) >= 65 and 
          subject == "english_word" and 
          obsClass.eql?(2)
            fluency['metBenchmark'] += 1
        end

        #check subject & benchmarks
        if Integer(ipm) >=17  and 
          subject == "kiswahili_word" and 
          obsClass.eql?(1)
            fluency['metBenchmark'] += 1
        end

        if Integer(ipm) >=45  and 
          subject == "kiswahili_word" and 
          obsClass.eql?(2)
            fluency['metBenchmark'] += 1
        end
        #check for english
        #if subject == 'english_word'
         # puts "Class: #{grade} - Ipm: #{fluency['itemsPerMinute']} MB: #{fluency['metBenchmark']}"
       # end 

        fluency['benchmarked'] += 1
      #end
    end
    
    return fluency
  end

  #
  # Compute correct Items per Minute For Each Trip
  #

  def fluencyRates(trip, grade, subject)

    #build array with fluency details for each trip
    results                     = trip['value']['items']
    fluency                   ||= {}
    fluency['itemsPerMinute'] ||= 0
    fluency['metBenchmark']   ||= 0
    fluency['benchmarked']    ||= 0
    totalItemsPerMinute = 0
    #
    #Get grid tests only from the trip values
    #

    results.map { | items |
      items['inputs'].map { |item|  

        if item['mode'] == 'TANGY_TIMED_MODE_DISABLED' and 
          gridFluencyRates = itemsPerMinute(item, grade, subject)
            
          if !gridFluencyRates.nil?
            fluency['itemsPerMinute'] += gridFluencyRates['itemsPerMinute']
            fluency['benchmarked']    += gridFluencyRates['benchmarked']
            fluency['metBenchmark']   += gridFluencyRates['metBenchmark']
          end
        end
      }
    }
    #puts "-----"
    return fluency
  end

  #
  #
  #  Validate the trip results against the constraints stored in the workflow 
  #
  #
  def validateTrip(trip, workflow)
    # return valid if validation not enabled for trip
    return true if not workflow
    return true if not workflow['enabled']
    return true if not workflow['constraints']

    #assume incomplete if there is no min and max time defined
    #return false if not trip['value']['minTime']
    #return false if not trip['value']['maxTime']

    if !@timezone.nil?
      startDate = Time.at(trip['value']['minTime'].to_i / 1000).getlocal(@timezone)
      endDate   = Time.at(trip['value']['maxTime'].to_i / 1000).getlocal(@timezone)
    else 
      startDate = Time.at(trip['value']['minTime'].to_i / 1000)
      endDate   = Time.at(trip['value']['maxTime'].to_i / 1000)
    end

    workflow['constraints'].each { | type, constraint |
      if type == "timeOfDay"
        startRange = constraint['startTime']['hour']
        endRange   = constraint['endTime']['hour']
        return false if not startDate.hour.between?(startRange, endRange)

      elsif type == "duration"
        if constraint["hours"]
          #return false if TimeDifference.between(startDate, endDate).in_hours < constraint["hours"]
        elsif constraint["minutes"]
          #return false if TimeDifference.between(startDate, endDate).in_minutes < constraint["minutes"]
        elsif constraint["seconds"]
          #return false if TimeDifference.between(startDate, endDate).in_seconds < constraint["seconds"]
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
