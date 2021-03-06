# encoding: utf-8

class Csv

  CACHE_SIZE = 500 # how many results to get
  FILE_BUFFER_SIZE = 32768 # 2 ^ 15 # how long the string will be before write to disk
  BASE_PATH = File.join( Dir.pwd, "csvs" )

  def initialize( options )

    @couch         = options[:couch]
    @name          = options[:name]
    @path          = options[:path]
    @locationList  = options[:locationList]
    @userList      = options[:userList]
    @cachedResults = {}

  end


  # lazyish get from server
  def getResult( id )

    # try to get it from the cache
    if @cachedResults[id].nil? # if the result isn't there get new ones

      nextResultIds = @orderedResults.slice!( 0, CACHE_SIZE )

      # fetch next results
      nextResults = @couch.postRequest({
        :view => "csvRows",
        :data => { "keys" => nextResultIds },
        :parseJson => true
      })

      # this doesn't keep the old results if there were any... @orderedResults important
      @cachedResults = Hash[nextResults['rows'].map { |row| [row['id'], row['value'] ] }]

    end

    result = @cachedResults[id]
    @cachedResults.delete(id)

  end

  def doWorkflow(options)

    resultsByTripId = options[:resultsByTripId]
    groupTimeZone = options[:groupTimeZone]

    machineNames = []
    columnNames  = []
    indexByMachineName = {}

    files = getFiles()

    # save all the result ids in order so we can can grab chunks
    @orderedResults = []
    resultsByTripId.each { | tripId, resultIds| @orderedResults.concat(resultIds) }

    # go through each trip and it's array of resultIds
    resultsByTripId.each { | tripId, resultIds |

      # make an array of resultIds for this trip
      results = resultIds.map { | resultId | getResult(resultId) }

      row = []
      results.each_with_index { | result, resultIndex |
        

        next if result.nil?

        for cell in result

          key         = cell['key']
          value       = cell['value']
          machineName = cell['machineName'] + resultIndex.to_s

          # hack for handling time
          isTimeRelated = key.match(/timestamp/) || key.match(/start_time/) || key.match(/startTime/)
          isntFalsy     = ! ( value.nil? || value == "" || value == 0 )
          if isTimeRelated && isntFalsy && groupTimeZone.nil?  then value = Time.at(value.to_i / 1000).strftime("%yy %mm %dd %Hh %Mm") end
          if isTimeRelated && isntFalsy && !groupTimeZone.nil? then value = Time.at(value.to_i / 1000).getlocal(groupTimeZone).strftime("%yy %mm %dd %Hh %Mm") end

          #hack for grabbing MPESA number along with enumerator
          requireUserFetch = key.match(/enumerator/)
          
          # Hack for handling location
          requireLocationFetch = key.match(/locationIndex/)

          if requireUserFetch then
            unless indexByMachineName["#{machineName}-enumerator"] # Have we seen the machine name before?
              machineNames.push "#{machineName}-enumerator"
              indexByMachineName["#{machineName}-enumerator"] = machineNames.index("#{machineName}-enumerator")
              columnNames.push key
            end
            index = indexByMachineName["#{machineName}-enumerator"]
            row[index] = value

            unless indexByMachineName["#{machineName}-role"] # Have we seen the machine name before?
              machineNames.push "#{machineName}-role"
              indexByMachineName["#{machineName}-role"] = machineNames.index("#{machineName}-role")
              columnNames.push "role"
            end
            index = indexByMachineName["#{machineName}-role"]
            tmpUser    = @userList.getUser(value)
            row[index] = (tmpUser["role"] || "---")
            
            unless indexByMachineName["#{machineName}-mpesa"] # Have we seen the machine name before?
              machineNames.push "#{machineName}-mpesa"
              indexByMachineName["#{machineName}-mpesa"] = machineNames.index("#{machineName}-mpesa")
              columnNames.push "mpesa"
            end
            index      = indexByMachineName["#{machineName}-mpesa"]
            tmpUser    = @userList.getUser(value)
            row[index] = (tmpUser["mpesaPhone"] || "---")

            unless indexByMachineName["#{machineName}-phone"] # Have we seen the machine name before?
              machineNames.push "#{machineName}-phone"
              indexByMachineName["#{machineName}-phone"] = machineNames.index("#{machineName}-phone")
              columnNames.push "phone"
            end
            index      = indexByMachineName["#{machineName}-phone"]
            tmpUser    = @userList.getUser(value)
            row[index] = (tmpUser["phone"] || "---")

          elsif requireLocationFetch then
            #puts "fetching location - locationIndex - #{value}"
            locationData = @locationList.retrieveLocation(value.split('-').last)
            #puts "locationData: #{locationData}"

            for pair in locationData
              locCol = pair.first.gsub "Label", "Name"
              locVal = pair.last

              unless indexByMachineName["#{machineName}-#{locCol}"] # Have we seen the machine name before?
                machineNames.push "#{machineName}-#{locCol}"
                indexByMachineName["#{machineName}-#{locCol}"] = machineNames.index("#{machineName}-#{locCol}")
                columnNames.push locCol
              end

              index = indexByMachineName["#{machineName}-#{locCol}"]
              row[index] = locVal
            end
          else

            # puts "Col: #{key}, Val: #{value}"
            unless indexByMachineName[machineName] # Have we seen the machine name before?
              machineNames.push machineName
              indexByMachineName[machineName] = machineNames.index(machineName)
              columnNames.push key
            end
            index = indexByMachineName[machineName]
            row[index] = value

          end
        end

      }

      files[:body].write row.map { |title| "\"#{title.to_s.gsub(/"/,'”')}\"" }.join(",") + "\n"

    }

    files[:header].write columnNames.map { |title| "\"#{title.to_s.gsub(/"/,'”')}\"" }.join(",") + "\n"

    files[:header].close()
    files[:body].close()

    `cat #{files[:headerUri]} #{files[:bodyUri]} > #{files[:fileUri]}`

    return { :uri => files[:fileUri], :name => files[:fileName] }

  end # of doWorkflow


  def doAssessment(options)

    resultIds = options[:resultIds]

    columnNames = []
    machineNames = []
    indexByMachineName = {}

    files = getFiles()

    # save all the result ids in order so we can can grab chunks
    @orderedResults = []
    resultIds.each { |value| @orderedResults.push value }

    resultIds.each { |resultId|

      row = []
      
      result = getResult(resultId)

      for cell in result  

        key         = cell['key']
        value       = cell['value']
        machineName = cell['machineName']

        unless indexByMachineName[machineName] # Have we seen the machine name before?
          machineNames.push machineName
          columnNames.push key
          indexByMachineName[machineName] = machineNames.index(machineName)
        end

        index = indexByMachineName[machineName]

        row[index] = value

      end

      files[:body].write row.map { |value| 
        value = value.to_s if value.class != String
        result = "\""
        (0..value.length-1).each { |index|
          result += if value[index] == '"' then '”' else value[index] end
        }
        result += "\""
      }.join(",") + "\n"

    }

    files[:header].write columnNames.map { |title| "\"#{title.to_s.gsub(/"/,'”')}\"" }.join(",") + "\n"

    files[:header].close()
    files[:body].close()
    `cat #{files[:headerUri]} #{files[:bodyUri]} > #{files[:fileUri]}`

    return { :uri => files[:fileUri], :name => files[:fileName] }

  end # of doAssessment

  private

  def getFiles()

    path = File.join( BASE_PATH, @path )

    # ensure group directory
    unless File.exists?( path )
      Dir.mkdir( path )
    end

    fileName = @name.downcase().gsub( /[^a-zA-Z0-9]/, "-" ) + ".csv"
    fileUri  = File.join( path, fileName )

    bodyFileName = @name.downcase().gsub( /[^a-zA-Z0-9]/, "-" ) + ".csv.body"
    bodyUri      = File.join( path, bodyFileName )
    bodyFile     = File.open( bodyUri, 'w' )

    headerFileName = @name.downcase().gsub( /[^a-zA-Z0-9]/, "-" ) + ".csv.header"
    headerUri      = File.join( path, headerFileName )
    headerFile     = File.open( headerUri, 'w' )

    return {
      :header  => headerFile, :headerUri => headerUri,
      :body    => bodyFile,   :bodyUri   => bodyUri,
      :fileUri => fileUri,    :fileName  => fileName
    }

  end

end
