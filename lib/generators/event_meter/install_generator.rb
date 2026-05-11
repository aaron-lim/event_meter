require "rails/generators"
require "rails/generators/active_record"

require_relative "../../event_meter/rails"

module EventMeter
  module Generators
    class InstallGenerator < ::Rails::Generators::Base
      include ::Rails::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      class_option :table_prefix,
        type: :string,
        default: "event_meter",
        desc: "PostgreSQL table prefix"

      class_option :namespace,
        type: :string,
        default: "event_meter:v1",
        desc: "EventMeter namespace"

      def copy_migration
        migration_template(
          "create_event_meter_tables.rb.erb",
          "db/migrate/create_event_meter_tables.rb"
        )
      end

      def create_initializer
        template "event_meter.rb.erb", "config/initializers/event_meter.rb"
      end

      def self.next_migration_number(dirname)
        ::ActiveRecord::Generators::Base.next_migration_number(dirname)
      end

      private

      def table_prefix
        options.fetch(:table_prefix)
      end

      def namespace
        options.fetch(:namespace)
      end

      def migration_sql
        EventMeter::Rails.migration_sql(table_prefix: table_prefix)
      end

      def indented_migration_sql
        migration_sql.lines.map { |line| "      #{line}" }.join
      end
    end
  end
end
