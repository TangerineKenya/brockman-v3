# encoding: utf-8

class Csv

  def initialize( options )

    @name = options[:name]
    @path = options[:path]

  end

  def doWorkflow(options)

    allResultsById  = options[:allResultsById]
    resultsByTripId = options[:resultsByTripId]

    machineNames = []
    columnNames  = []

    files = getFiles()

    resultsByTripId.each { | tripId, resultIds |

      results = resultIds.map { | resultId | allResultsById[resultId] }

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
          if isTimeRelated && isntFalsy then value = Time.at(value.to_i / 1000).strftime("%yy %mm %dd %Hh %Mm") end

          unless machineNames.include?(machineName)
            machineNames.push machineName
            columnNames.push key
          end

          index = machineNames.index(machineName)

          row[index] = value

        end

      }

      files[:body].write row.map { |title| "\"#{title.to_s.gsub(/"/,'”')}\"" }.join(",") + "\n"

    }

    files[:header].write columnNames.map { |title| "\"#{title.to_s.gsub(/"/,'”')}\"" }.join(",") + "\n"

    files[:header].close()
    files[:body].close()

    `cat #{files[:headerUri]} #{files[:bodyUri]} > #{files[:fileUri]}`

    return { :uri => files[:fileUri], :name => files[:fileName] }

  end

  private

  def getFiles()

    # ensure group directory
    unless File.exists?( @path )
      Dir.mkdir( @path )
    end

    fileName = @name.downcase().gsub( " ", "-" ) + ".csv"
    fileUri  = File.join( @path, fileName )

    bodyFileName = @name.downcase().gsub( " ", "-" ) + ".csv.body"
    bodyUri      = File.join( @path, bodyFileName )
    bodyFile     = File.open( bodyUri, 'w' )

    headerFileName = @name.downcase().gsub( " ", "-" ) + ".csv.header"
    headerUri      = File.join( @path, headerFileName )
    headerFile     = File.open( headerUri, 'w' )

    return {
      :header  => headerFile, :headerUri => headerUri,
      :body    => bodyFile,   :bodyUri   => bodyUri,
      :fileUri => fileUri,    :fileName  => fileName
    }

  end

end