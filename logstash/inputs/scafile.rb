# encoding: utf-8                                                               

############################################
#
# SCAFILE
# 
# Logstash mediation input for SCAPI
# Facilites working with series of files, each of which are opened, 
# data extracted, and then moved to a designated 'done' directory. 
# (this is more a file processing metaphor more than streaming)
#
# Version 120115.1 Robert Mckeown
#
############################################

                                         
require "logstash/inputs/base"
require "logstash/namespace"
require 'set'
require "pathname"

class LogStash::Inputs::SCAFile < LogStash::Inputs::Base
  config_name "scafile"
  milestone 1

  default :codec, "plain"

  config :path,        :validate => :string, :required => true
  config :done_dir, :validate => :string, :required => true
         # files moves here af
  config :poll_interval, :validate => :number, :default => 10
         # if true, input files are processed in groups with the same prefix e.g. timestamp__filename.txt

  public
  def register 

  end

  public
  def processFiles(queue,workingFiles)

      workingFiles.each do | filename |

      @logger.debug("Processing file " + filename + "\n")

      begin

        File.read(filename).lines.each do | line | 

          event = LogStash::Event.new("message" => line)
          decorate(event)

          queue << event
        end

        event = LogStash::Event.new("message" => "SCAWindowMarker")
        decorate(event)
        queue << event

        realdirpath = File.dirname(File.realdirpath(filename))
        filebasename = File.basename(filename)
        File.rename(realdirpath + "/" + filebasename, @done_dir + "/" + filebasename)

      rescue
        @logger.warn("Exception processing file " + filename + "\n")
        @logger.warn("Line = " + line)

      end

      end 

  end


  public
  def run(queue)

    loop do
      @logger.debug("Scanning for files in ", :path => @path)

      dirFiles = Dir.glob(@path).sort   # Process them in alphabetical order, 

      processFiles(queue,dirFiles)     

      sleep(@poll_interval)
    end # loop

    finished
  end

  public
  def teardown
  end

end # class LogStash::Inputs::SCAFile
