
class CouchIterator

  def initialize (options)
    @couch          = options[:couch]
    @requestOptions = options[:requestOptions]
    @chunkSize      = options[:chunkSize] || 500
    @index          = options[:index]     || 0
    @method         = options[:method]    || "get"
    @totalRows      = nil
  end

  def refreshTotalRows
    refreshOptions = @requestOptions.merge( { :params => { :limit => 1, :refresher => true } } )
    return @totalRows = doRequest(refreshOptions)['total_rows']
  end

  def hasChunk()
    if @chunkSize * @index < @totalRows
      return true
    else
      return false
    end
  end

  def getChunk()
    requestIndex = @chunkSize * @index
    chunkyOptions = @requestOptions.merge( { :params => { :limit => @chunkSize, :skip => requestIndex } } )
    response = doRequest(chunkyOptions)
    @index += 1
    return response
  end

  def doRequest(options)
    if @method == "get"
      return JSON.parse(@couch.getRequest(options).to_json)
    elsif @method == "post"
      return JSON.parse(@couch.postRequest(options).to_json)
    end
  end

end
