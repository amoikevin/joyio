package org.joy.io.persist;

public class BoundGenerator {
	public static class Bound<T extends Comparable<T>> {
		T upper;
		boolean upInclusive;
		T lower;
		boolean lowInclusive;

		public Bound(boolean lowInclusive, T lower, boolean upInclusive,
				T upper) {
			super();
			this.lowInclusive = lowInclusive;
			this.lower = lower;
			this.upInclusive = upInclusive;
			this.upper = upper;
		}

		public Bound<T> intersect(Bound<T> b) {
			T resLower = b.lower;
			T resUpper = b.upper;
			boolean resUpInclusive = false;
			boolean resLowInclusive = false;
			if (b.lower.compareTo(lower) > 0) {
				resLower = b.lower;
				resLowInclusive = b.lowInclusive;
			} else if (b.lower.compareTo(lower) < 0) {
				resLower = lower;
				resLowInclusive = lowInclusive;
			} else {
				// 判断是否是包含下界
				if (b.lowInclusive && lowInclusive)
					resLowInclusive = true;
				else
					resLowInclusive = false;
			}

			if (b.upper.compareTo(upper) < 0) {
				resUpper = b.upper;
				resUpInclusive = b.upInclusive;
			} else if (b.upper.compareTo(upper) > 0) {
				resUpper = upper;
				resUpInclusive =b.upInclusive; 
			} else {
				// 判断是否是包含下界
				if (b.upInclusive && upInclusive)
					resUpInclusive = true;
				else
					resUpInclusive = false;
			}
			return new Bound<T>(resLowInclusive, resLower, resUpInclusive,
					resUpper);
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return "up "+upper+ " lower "+lower+" ui "+upInclusive+" li "+lowInclusive;
		}
	}

	private Bound<?> b;

	// private Bound<?> old;

	public BoundGenerator(Bound old, String op, String value) {
		// 分析Value类型
		if (value.endsWith("L")) {
			if(old==null){
				old = new Bound<Long>(true, Long.MIN_VALUE, true,
						Long.MAX_VALUE);
			}
			// Long型
			Long realVal = Long.parseLong(value
					.substring(0, value.length() - 1));
			Bound<Long> t = new Bound<Long>(true, Long.MIN_VALUE, true,
					Long.MAX_VALUE);
			if (op.equals("=")) {
				t.lower = t.upper = realVal;
				t.lowInclusive = t.upInclusive = true;
			} else if (op.equals(">")) {
				t.lower = realVal;
				t.lowInclusive = false;
			} else if (op.equals("<")) {
				t.upper = realVal;
				t.upInclusive = false;
			} else if (op.equals(">=")) {
				t.lower = realVal;
				t.lowInclusive = true;
			} else if (op.equals("<=")) {
				t.upper = realVal;
				t.upInclusive = true;
			}
			b = old.intersect(t);
		} else if (value.endsWith("D")) {
			// DOUBLE型
			Double realVal = Double.parseDouble(value.substring(0, value
					.length() - 1));
			if(old==null){
				old = new Bound<Double>(true, Double.MIN_VALUE, true,
						Double.MAX_VALUE);
			}
			Bound<Double> t = new Bound<Double>(true, Double.MIN_VALUE, true,
					Double.MAX_VALUE);

			if (op.equals("=")) {
				t.lower = t.upper = realVal;
				t.lowInclusive = t.upInclusive = true;
			} else if (op.equals(">")) {
				t.lower = realVal;
				t.lowInclusive = false;
			} else if (op.equals("<")) {
				t.upper = realVal;
				t.upInclusive = false;
			} else if (op.equals(">=")) {
				t.lower = realVal;
				t.lowInclusive = true;
			} else if (op.equals("<=")) {
				t.upper = realVal;
				t.upInclusive = true;
			}
			b = old.intersect(t);
		} else if (value.endsWith("F")) {
			// Float型
			Float realVal = Float.parseFloat(value.substring(0,
					value.length() - 1));
			if(old==null){
				old = new Bound<Float>(true, Float.MIN_VALUE, true,
						Float.MAX_VALUE);
			}
			Bound<Float> t = new Bound<Float>(true, Float.MIN_VALUE, true,
					Float.MAX_VALUE);

			if (op.equals("=")) {
				t.lower = t.upper = realVal;
				t.lowInclusive = t.upInclusive = true;
			} else if (op.equals(">")) {
				t.lower = realVal;
				t.lowInclusive = false;
			} else if (op.equals("<")) {
				t.upper = realVal;
				t.upInclusive = false;
			} else if (op.equals(">=")) {
				t.lower = realVal;
				t.lowInclusive = true;
			} else if (op.equals("<=")) {
				t.upper = realVal;
				t.upInclusive = true;
			}
			b = old.intersect(t);
		} else {
			//Integer型
			Integer realVal = Integer.parseInt(value);
			if(old==null){
				old = new Bound<Integer>(true, Integer.MIN_VALUE, true,
						Integer.MAX_VALUE);
			}
			Bound<Integer> t = new Bound<Integer>(true, Integer.MIN_VALUE, true,
					Integer.MAX_VALUE);

			if (op.equals("=")) {
				t.lower = t.upper = realVal;
				t.lowInclusive = t.upInclusive = true;
			} else if (op.equals(">")) {
				t.lower = realVal;
				t.lowInclusive = false;
			} else if (op.equals("<")) {
				t.upper = realVal;
				t.upInclusive = false;
			} else if (op.equals(">=")) {
				t.lower = realVal;
				t.lowInclusive = true;
			} else if (op.equals("<=")) {
				t.upper = realVal;
				t.upInclusive = true;
			}
			b = old.intersect(t);
		}
	}

	public Bound<?> getBounds() {
		return b;
	}

}
