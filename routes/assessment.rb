
# Make CSVs for regular assessments
class Brockman < Sinatra::Base

  get '/assessment/:group/:assessmentId' do | group, assessmentId |

    requestId = SecureRandom.base64

    couch = Couch.new({
      :host      => $settings[:host],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    #
    # Authentication
    #

    authenticate = couch.authenticate(cookies)

    unless authenticate[:valid] == true
      $logger.info "Authentication failed"
      status 401
      return { :error => "not logged in" }.to_json
    end

    groupPath = "group-#{group.gsub(/group-/,'')}"

    assessmentName = JSON.parse(RestClient.get("http://#{$settings[:login]}@#{$settings[:host]}/#{groupPath}/#{assessmentId}"))['name']

    # Get csv rows for klass
    csvData = {
      :keys => [assessmentId]
    }

    csvRowResponse = JSON.parse(RestClient.post("http://#{$settings[:login]}@#{$settings[:host]}/#{groupPath}/_design/ojai/_view/csvRows",csvData.to_json, :content_type => :json,:accept => :json ))

    columnNames = []
    machineNames = []
    csvRows = []

    puts csvRowResponse.to_json

    for result in csvRowResponse['rows']

      row = []

      for cell in result['value']

        key         = cell['key']
        value       = cell['value']
        machineName = cell['machineName']
        unless machineNames.include?(machineName)
          machineNames.push machineName
          columnNames.push key
        end

        index = machineNames.index(machineName)

        row[index] = value

      end

      csvRows.push row

    end

    csvRows.unshift(columnNames)
    csvData = ""
    for row in csvRows
      csvData += row.map { |title|
        "\"#{title.to_s.gsub(/"/,'”')}\""
      }.join(",") + "\n"
    end

    unless params[:download] == "false"
      response.headers["Content-Disposition"] = "attachment;filename=#{assessmentName} #{timestamp()}.csv"
      response.headers["Content-Type"] = "application/octet-stream"
    end


    return csvData



  end


end