class Array
  def fuzzy_filter(h)
    keep_if do |item|
      raise "All elements in this array need to be of type Hash: (#{item.class}) #{item.inspect}" if item.class != Hash
      h.keys.all? { |k| h[k] === item[k] }
    end
  end
end
