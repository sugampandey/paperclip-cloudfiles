module Paperclip
  module Storage
    # Rackspace's Cloud Files service is a scalable, easy place to store files for
    # distribution, and is integrated into the Limelight CDN. You can find out more about 
    # it at http://www.rackspacecloud.com/cloud_hosting_products/files
    #
    # To install the Cloud Files gem, add the Gemcutter gem source ("gem sources -a http://gemcutter.org"), then
    # do a "gem install cloudfiles".  For more information, see the github repository at http://github.com/rackspace/ruby-cloudfiles/
    #
    # There are a few Cloud Files-specific options for has_attached_file:
    # * +cloudfiles_credentials+: Takes a path, a File, or a Hash. The path (or File) must point
    #   to a YAML file containing the +username+ and +api_key+ that Rackspace
    #   gives you. Rackspace customers using the cloudfiles gem >= 1.4.1 can also set a servicenet
    #   variable to true to send traffic over the unbilled internal Rackspace service network.
    #   You can 'environment-space' this just like you do to your
    #   database.yml file, so different environments can use different accounts:
    #     development:
    #       username: hayley
    #       api_key: a7f... 
    #     test:
    #       username: katherine
    #       api_key: 7fa... 
    #     production:
    #       username: minter
    #       api_key: 87k... 
    #       servicenet: true
    #   This is not required, however, and the file may simply look like this:
    #     username: minter...
    #     api_key: 11q... 
    #   In which case, those access keys will be used in all environments. You can also
    #   put your container name in this file, instead of adding it to the code directly.
    #   This is useful when you want the same account but a different container for 
    #   development versus production.
    # * +container+: This is the name of the Cloud Files container that will store your files. 
    #   This container should be marked "public" so that the files are available to the world at large.
    #   If the container does not exist, it will be created and marked public.
    # * +path+: This is the path under the container in which the file will be stored. The
    #   CDN URL will be constructed from the CDN identifier for the container and the path. This is what 
    #   you will want to interpolate. Keys should be unique, like filenames, and despite the fact that
    #   Cloud Files (strictly speaking) does not support directories, you can still use a / to
    #   separate parts of your file name, and they will show up in the URL structure.
    module CloudFile
      def self.extended base
        require 'cloudfiles'
        require 'mime/types'
        @@container ||= {}
        base.instance_eval do
          @cloudfiles_credentials = parse_credentials(@options[:cloudfiles_credentials])
          @container_name         = @options[:container]              || @cloudfiles_credentials[:container]
          @container_name         = @container_name.call(self) if @container_name.is_a?(Proc)
          @cloudfiles_options     = @options[:cloudfiles_options]     || {}
          @@cdn_url               = cloudfiles_container.cdn_url
          @path_filename          = ":cf_path_filename" unless @url.to_s.match(/^:cf.*filename$/)
          @url = @@cdn_url + "/#{URI.encode(@path_filename).gsub(/&/,'%26')}"
          @path = (Paperclip::Attachment.default_options[:path] == @options[:path]) ? ":attachment/:id/:style/:basename.:extension" : @options[:path]
        end
          Paperclip.interpolates(:cf_path_filename) do |attachment, style|
            attachment.path(style)
          end
      end
      
      def reconnect_cloudfiles
      
        @@cf = CloudFiles::Connection.new(@cloudfiles_credentials[:username], @cloudfiles_credentials[:api_key], true, @cloudfiles_credentials[:servicenet])
      end
      
      def cloudfiles
        @@cf ||= CloudFiles::Connection.new(@cloudfiles_credentials[:username], @cloudfiles_credentials[:api_key], true, @cloudfiles_credentials[:servicenet])
      end

      def create_container
        container = cloudfiles.create_container(@container_name)
        container.make_public
        container
      end
      
      def cloudfiles_container
        @@container[@container_name] ||= create_container
      end

      def container_name
        @container_name
      end

      def parse_credentials creds
        creds = find_credentials(creds).stringify_keys
        (creds[Rails.env] || creds).symbolize_keys
      end
      
      def exists?(style = default_style)
        cloudfiles_container.object_exists?(path(style))
      end

      # Returns representation of the data of the file assigned to the given
      # style, in the format most representative of the current storage.
      def to_file style = default_style
        @queued_for_write[style] || cloudfiles_container.create_object(path(style))
      end
      alias_method :to_io, :to_file

      def flush_writes #:nodoc:
        Rails.logger.info "*** flush_writes attachment ***"
        @queued_for_write.each do |style, file|
            retry_times = 0
            saved = false
            while saved == false and retry_times < 3
              Rails.logger.info "** flush_writes style: #{style} file: #{file} path:#{path(style)} retry_time => #{retry_times} **"
              begin 
                Rails.logger.info "** creating cloudfiles object **"
                object = cloudfiles_container.create_object(path(style),false)
                mime_types = MIME::Types.type_for(path(style))
                
                Rails.logger.info "** saving file cloudfiles **"
                if mime_types.first.nil?
                  object.load_from_filename(file.path, {}, true)
                else
                  content_type_to_write = mime_types.first.to_s
                  content_type_to_write = "text/javascript" if content_type_to_write == "application/javascript"
                  object.load_from_filename(file.path, {'Content-Type' => content_type_to_write}, true)
                end
                saved = true
              rescue CloudFiles::Exception::InvalidResponse, CloudFiles::Exception::Connection
                Rails.logger.info "** CloudFiles::Exception::InvalidResponse, CloudFiles::Exception::Connection Raised **"
                reconnect_cloudfiles
                saved = false
                retry_times += 1
              end
            end
        end
        @queued_for_write = {}
      end

      def flush_deletes #:nodoc:
        @queued_for_delete.each do |path|
          cloudfiles_container.delete_object(path)
        end
        @queued_for_delete = []
      end
      
      def find_credentials creds
        case creds
        when File
          YAML.load_file(creds.path)
        when String
          YAML.load_file(creds)
        when Hash
          creds
        else
          raise ArgumentError, "Credentials are not a path, file, or hash."
        end
      end
      private :find_credentials

    end
    
  end
end
