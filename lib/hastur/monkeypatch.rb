class Array
  def fuzzy_filter(h)
    keep_if do |item|
      h.keys.all? { |k| h[k] === item[k] }
    end
  end
end
