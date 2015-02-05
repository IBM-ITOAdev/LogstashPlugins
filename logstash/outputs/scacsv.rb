############################################                                    
#                                                                               
# Scacsv                               
#                                                                               
# Logstash mediation output for SCAPI
#                                                                               
# Version 081214.2 Robert Mckeown                                               
#                                                                               
############################################     

require "csv"
require "logstash/namespace"
require "logstash/outputs/file"

# SCACSV - based upon original Logstash CSV output.
# 
# Write events to disk in CSV format
# Write a PI header as the first line in the file
# Name file per PI convention, based upon first and last timestamps encountered 

class LogStash::Outputs::SCACSV < LogStash::Outputs::File

  config_name "scacsv"
  milestone 1

  # The field names from the event that should be written to the CSV file.
  # Fields are written to the CSV in the same order as the array.
  # If a field does not exist on the event, an empty string will be written.
  config :fields, :validate => :array, :required => true


  # If present, the values here will over-ride the default header
  # names. Useful if you simply want to provide other names
  config :header, :validate => :array, :required => false
  
  # Options for CSV output. This is passed directly to the Ruby stdlib to\_csv function. 
  # Full documentation is available here: [http://ruby-doc.org/stdlib-2.0.0/libdoc/csv/rdoc/index.html].
  # A typical use case would be to use alternative column or row seperators eg: `csv_options => {"col_sep" => "\t" "row_sep" => "\r\n"}` gives tab seperated data with windows line endings
  config :csv_options, :validate => :hash, :required => false, :default => Hash.new

  # Name of the output group - used as a prefix in the renamed file
  config :group, :validate  => :string, :required => true
  config :max_size, :validate => :number, :default => 0
  config :flush_interval, :validate => :number, :default => 60
  config :time_field, :validate => :string, :default => "timestamp"
  config :time_format, :validate => :string, :default => "%Y%m%d%H%M%S"
  config :tz_offset, :validate => :number, :default => 0
  config :increment_time, :validate => :boolean, :default => false
  config :keep_original_timestamps, :validate => :boolean, :default => false

  public
  def register
    super
    @csv_options = Hash[@csv_options.map{|(k,v)|[k.to_sym, v]}]
    
    # variables to hold the start and end times which we'll use to rename the files to
    @startTime   = "missingStartTime"
    @endTime     = "missingEndTime"
    @recordCount = 0

    @lastOutputTime = 0
    @flushInterval = @flush_interval.to_i

    @timerThread = Thread.new { flushWatchdog(@flush_interval) }

  end

  # This thread ensures that we output (close and rename) a file every so often
  private
  def flushWatchdog(delay)
    begin
      @logger.debug("SCACSVFlushWatchdog - Last output time = " + @lastOutputTime.to_s)
      while true do
        @logger.debug("SCACSVFlushWatchdog - Time.now = " + Time.now.to_s + " $lastOutputTime=" + @lastOutputTime.to_s + " delay=" + delay.to_s)

        if ( (Time.now.to_i >= (@lastOutputTime.to_i + delay.to_i)) and (@recordCount > 0)) then
          @logger.debug("SCACSVFlushWatchdog - closeAndRenameCurrentFile")
          closeAndRenameCurrentFile
        end
        @logger.debug("SCACSVFlushWatchdog - Sleeping")
        sleep 1
      end
    end
  end

  public
  def receive(event)
    return unless output?(event)

    @logger.debug("in SCACSV receive")

    if (event['message'] == "SCAWindowMarker") and (@recordCount >= 1)
        closeAndRenameCurrentFile
    else
      @formattedPath = event.sprintf(@path)
      fd = open(@formattedPath)
      @logger.debug("SCACSVreceive - after opening fd=" + fd.to_s)

      if @recordCount == 0
        # output header on first line - note, need a minimum of one record for sensible output
        if @header then 
#         csv_header = @fields.map { |name| name }
          fd.write(@header.to_csv(@csv_options))
        else     
          fd.write(@fields.to_csv(@csv_options))
        end
      end

      csv_values = @fields.map {|name| get_value(name, event)}
      fd.write(csv_values.to_csv(@csv_options))

      flush(fd)
      close_stale_files

      # remember state
      @recordCount = @recordCount + 1
      @lastOutputTime = Time.now

      # capture the earliest - assumption is that records are in order
      if (@recordCount) == 1 
        @startTime = event[@time_field]
      end

      # for every record, update endTime - again, assumption is that records are in order
      @endTime = event[@time_field]

      if ((@max_size > 0) and (@recordCount >= max_size))
        # Have enough records, close it out
        closeAndRenameCurrentFile
      end

    end  

#      @logger.debug("SCACSV startTime" + @startTime)
#      @logger.debug("SCACSV endTime" + @endTime)

  end #def receive

  private
  def get_value(name, event)
    val = event[name]
    case val
      when Hash
        return val.to_json
      else
        return val
    end
  end

  def closeAndRenameCurrentFile

    # cloned and changed from the 'file.rb' operatore
    # even though this is in a loop - assumption is that we have one file here for the SCA CSV use
    @files.each do |path, fd|
      begin
        fd.close
        @files.delete(path) # so it will be forgotten and we can open it up again if needed
        @logger.debug("closeAndRenameCurrentFile #{path}", :fd => fd)

        # Now the various time adjustments

        if @time_format != ""
          # only attempt this if we are not keeping the original timestamps
          # assumption here is we have epoch times
          @startTime = (@startTime.to_i + @tz_offset).to_s
          @endTime   = (@endTime.to_i + @tz_offset).to_s
        end

        if (@increment_time & !@endTime.nil?)
          # increment is used to ensure that the end-time on the filename is after the last data value
          @endTime = (@endTime.to_i + 1).to_s
        end

        if @startTime.nil?  
          @logger.debug("SCACSV missing start time for + #{group}")
          @startTime = "noStartTime"
        else
          if @time_format != "" then  #output format supplied, convert to that
            @startTime = DateTime.strptime(@startTime,"%s").strftime(@time_format)
          end
        end

        if @endTime.nil? then 
          @logger.debug("SCACSV missing end time for  + #{group}")
          @endTime = "noEndTime"
        else
          if @time_format != "" then  #output format supplied, convert to that
            @endTime = DateTime.strptime(@endTime,"%s").strftime(@time_format)       
          end
        end

        newFilename = "#{group}" + "__" + @startTime + "__" + @endTime + ".csv"

        if newFilename.include? '/'
          @logger.error("New filename " + newFilename + " cannot contain / characters. Check the timestamp format. / characters stripped from filename")
          newFilename = newFilename.delete! '/'
        end

        realdirpath = File.dirname(File.realdirpath("#{path}"))
        realdirpath = File.dirname(File.realdirpath(path))
        oldFilename = File.basename(path)

        File.rename(realdirpath + "/" + oldFilename, realdirpath + "/" + newFilename)
        
        # reset record count so we'll pick up new start time, and put a header on next file 
        # when a new record comes in
        @recordCount = 0
        @lastOutputTime = Time.now

      rescue Exception => e
        @logger.error("Exception while flushing and closing files.", :exception => e)
      end
    end

  end

  def teardown
     @logger.debug("SCACSV - Teardown: closing files")

    Thread.kill(@timerThread)
    closeAndRenameCurrentFile

    finished
  end

end # class LogStash::Outputs::SCACSV

