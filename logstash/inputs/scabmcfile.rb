# encoding: utf-8                                                               

############################################
#
# SCABMCFILE
# 
# Logstash mediation filter for SCAPI/BMC patrol
#
# Version 090215.1 Robert Mckeown
#
############################################

                                         
require "logstash/inputs/base"
require "logstash/namespace"
require 'set'
require "pathname"

class LogStash::Inputs::SCABMCFile < LogStash::Inputs::Base
  config_name "scabmcfile"
  milestone 1

  default :codec, "plain"

  config :path,        :validate => :string, :required => true
  config :done_dir, :validate => :string, :required => true
  config :ready_file, :validate => :string, :default => ""
  config :tz_offset, :validate => :number, :default => 0
  config :groups, :validate => :array, :default => []
  config :poll_interval, :validate => :number, :default => 10
  config :group_inputs, :validate => :boolean, :default => false 
         # if true, input files are processed in groups with the same prefix e.g. timestamp__filename.txt
  config :pivot, :validate => :boolean, :default => true
  config :sort, :validate => :string, :default => "" # rmck: if specified, use specified external sort (e.g. /bin/sort) - not yet functional

  public
  def register 
    @kpiID = ""
    @k = Hash.new
    @bufferedEvents = []

    @internalSort = false

    if @sort == "" 
      @internalSort = true
    else
      @internalSort = false
    end

    @eventCount = 0

  end

  public
  def processFiles(queue,workingFiles)

      workingFiles.each do | filename |

puts("Processing file " + filename + "\n")

        File.read(filename).lines.each do | line | 

          if line.include? "Total matched parameters:"  
            # End of set
          elsif line =~ /^\t/ 
            # data line starts with tab
            if  (@groups.empty? or @groups.include?(@k['group']))

              # then only process data line if we didn't specify groups to include
              # (ie include all groups) or if we did specify groups, then
              # the group should be one of the specified groups

              begin

                event = LogStash::Event.new("message" => line.gsub(/\r|\n/,''))
                decorate(event)
 
                # Need to figure out correct end of line \r or \n
#            m = /\t(?<timestamp>.*) (?<value>.*)\r/.match(event['message'])
                m = /\t(?<timestamp>.*) (?<value>.*)/.match(event['message']) 
                epochTime = (DateTime.parse(m['timestamp'])).to_time.to_i + @tz_offset

                event['epoch'] = epochTime.to_s
                event['timestamp'] = m['timestamp']
                event['kpiID'] = @kpiID
                event['value'] = m['value']

                # Assumption is that K is already populated, providing context
                event['node']     = @k['node']
                event['group']    = @k['group']
                event['instance'] = @k['instance']
                event['metric']   = @k['metric']
  
# This is the place to filter upstream


                event['message'] = 
                event['epoch'] + "," + 
                event['timestamp'] + "," + 
                event['node'] + "," + 
                event['group'] + "," +
                event['instance'] + "," +
                event['metric'] + "," +
                event['value'] 

                if @internalSort
                  @bufferedEvents.push(event)
                else
                  puts("would output " + event['message'] + "\n")
                end
                @eventCount = @eventCount+1

              rescue StandardError=>e
                puts("Error: #{e}  while processing line " + line + "\n")
              end
            end
          else
            # no tab, it's a KPI ID
            @kpiID = line.gsub(/\r|\n/,'')
            @k = /(?<node>.*)\/(?<group>.*)\.(?<instance>.*)\/(?<metric>.*)/.match(@kpiID)
          end
        end # File.read each line loop

        realdirpath = File.dirname(File.realdirpath(filename))
        filebasename = File.basename(filename)  
        File.rename(realdirpath + "/" + filebasename, @done_dir + "/" + filebasename)

        if @internalSort then
          puts("Buffered events size = " + @bufferedEvents.size.to_s + "\n")
        else
          puts("Output events size = " + @eventCount.to_s + "\n")
        end
      end   # Dir.glob  do

      if @eventCount > 0 
      # there is some data to process

        if @internalSort then
          puts("About to sort " + @bufferedEvents.size.to_s + " rows\n")
          @bufferedEvents.sort! { |a,b| a['epoch'] <=> b['epoch'] }
          puts("Sorted\n")
        else
          puts("External sort\n")
        end

        if @pivot
          puts("Pivoting " + @bufferedEvents.length.to_s + "\n")
          pivotedEvents = pivotForAllGroups(@bufferedEvents)
          puts("Pivoting " + @bufferedEvents.length.to_s + " original events to " + 
              pivotedEvents.length.to_s + " events\n")
        else
          puts("Skipping pivot. Assigning resource/metric identities for " + @bufferedEvents.length.to_s + " events\n")
          pivotedEvents = @bufferedEvents.map { |e|        
            e['piResource'] = e['node'] + "_" + e['instance']
            e['piMetric']   = e['group'] + "_" + e['metric']
            e
          }
          puts("Finished assigning resource/metric identities\n")
        end

        # Clear for next time around
        @bufferedEvents.clear
        @eventCount = 0

        # SCAWindowMarker added at end
        event = LogStash::Event.new("message" => "SCAWindowMarker")
        decorate(event)
        pivotedEvents.push(event)

        pivotedEvents.each do | sortedEvent |
          queue << sortedEvent
        end
      end # @eventCount  >0


  end



  public
  def run(queue)

    loop do

      @logger.debug("Scanning for files in ", :path => @path)


 #      File.delete(@ready_file) # remove marker file 
      dirFiles = Dir.glob(@path)

      if (@ready_file != "" and File.exist?(@ready_file)) or
         (@ready_file == "")  then

         # Ok to process files
         # cleanup just in case
         File.delete(@ready_file)  if File.exists?(@ready_file)  
         dirFiles = Dir.glob(@path) # todo: potential for new read_file to appear between this and previous line
                                    # remove refs to that file in the dirFiles

        if @group_inputs
          # Gather input files by common prefix (usually, timestamp e.g. <ts1>__filname.txt
          # process the files with a common prefix together, then move on to the next set

          groupsKey = (dirFiles.collect do |e| e.split("__").first end).sort.uniq

          filesByGroup = Hash.new
          groupsKey.each do |g|
            filesByGroup[g] =  dirFiles.select do |e|
              g == e.split("__").first
            end
          end

          groupsKey.each do |g|
            puts("Processing group " + g + "\n")
            processFiles(queue,filesByGroup[g])
          end
        else
          # just process them all as ibe set 
          processFiles(queue,dirFiles)     
        end
      end

# end process files

      sleep(@poll_interval)
    end # loop

    finished
  end

  public
  def teardown
  end

  private 
  def pivotForAllGroups( events )

    currentTimestamp = Hash.new
    pending = Hash.new
    groupsAlreadySeen = Set.new
    output = []

    # Fixed config for BMC - modelled after stand-alone pivot
    window_column = "timestamp"
    fixed_columns = ["epoch","timestamp","node","instance","group"]
    target_column = "metric"
    value_column  = "value"

    events.each do | event |

      currentGroup = event['group']

      if !groupsAlreadySeen.include?(currentGroup)
        pending[currentGroup] = Set.new
        groupsAlreadySeen.add( event['group'] ) 
      end

      if currentTimestamp[ currentGroup ].nil?
        pending[currentGroup].add(event)
        currentTimestamp[currentGroup] = event[window_column]
      elsif event[window_column] == currentTimestamp[currentGroup]
        pending[currentGroup].add(event)
      else
        # different window column value, time to pivot

        pivoted = pivot(pending[currentGroup], target_column, value_column, fixed_columns)
        output.concat(pivoted)     

        pending[currentGroup].clear
        pending[currentGroup].add(event)
        currentTimestamp[currentGroup] = event[window_column]

      end # if !groupsAlreadySeen
    end # events.each do

    # Gone through all events,  check pending for groups for any remaining ones that need to be flushed out
    groupsAlreadySeen.each do | currentGroup |
      if pending[currentGroup].size > 0 
        # some remaining to pivot
        pivoted = pivot(pending[currentGroup], target_column, value_column, fixed_columns)
        output.concat(pivoted)
      end
    end
 


    return output
  end


  private
  def pivot(events, targetColumn, valueColumn, fixedColumns)

    @logger.debug("*** SCAPivot in pivot \n")
    @logger.debug("*** events.size=" + events.size.to_s + "\n")
    @logger.debug("*** fixedColumns=" + fixedColumns.to_s + "\n")

    uniqueFixedColumnAssocs = uniqueRowsForFixedColumns(events,fixedColumns)

    @logger.debug("*** uniqueFixedColumnAssocs.size=" + uniqueFixedColumnAssocs.size.to_s + "\n")

    # uniqueGroups is Array of Arrays
    uniqueGroups =
      uniqueFixedColumnAssocs.collect {
        | keyColumnAssoc |
      events.select {
        | event |
        matchesFixedColumn?(event,keyColumnAssoc)
      }
    }

    # First hash                                                                     
    pivotedEvents =
      uniqueGroups.collect {
      | group |

      # Make a new event, plucking fixed column values from the first entry                                                                
      newEvent = newEventCorrespondingToFixedColumns(group.first,fixedColumns)

      group.each do | event |

        if (event[targetColumn].nil? || event[valueColumn].nil?)   
            @logger.debug("NilEventTargetOrValueColumn - Dropping Event" + event.to_json.to_s  ) 
            # shouldn't really have an event reaching here without a targetColumn value.
            # This is indicative of processing problems upstream
            # Just to keep things going, replace the nil with an empty string
        else
            newEvent[event[targetColumn]]=event[valueColumn]
        end
      end
      newEvent
    }

    return pivotedEvents
  end

  private
  def newEventCorrespondingToFixedColumns( event, fixedColumns )

      #  newHashEvent = Hash.new
      newHashEvent = LogStash::Event.new("message" => "Pivoted from originals") 

      fixedColumns.each do | key |
 
      if (event[key].nil?) 
        newHashEvent[key] = "" # stick an empty string there, just in case 
      else
        newHashEvent[key] = event[key]
      end
    end

    return newHashEvent

  end

  private
  def matchesFixedColumn?(event,keyColumnAssoc)

    keyColumnAssoc.each {

      | colKey,colValue |
      if ( !(event[colKey] == colValue ))
        return false
      end
    }

    return true

  end

  private
  def uniqueRowsForFixedColumns(events,fixedColumns)

    hashForFixedColumns = events.collect {
     |event|
      h = Hash.new
      fixedColumns.collect {
         |key|
         h[key] = event[key]
      }
      h
    }

    @logger.debug("hashForFixedColumns=" + hashForFixedColumns.size.to_s)

    # Use Set's uniqueness to get the unique set                                                                                                            
    uniqueAssocs = Set.new
    hashForFixedColumns.each do | assoc |
      uniqueAssocs.add(assoc)
    end

    @logger.debug("uniqAssocs=" + uniqueAssocs.size.to_s)

    return uniqueAssocs

  end

end # class LogStash::Inputs::SCABMCFile
